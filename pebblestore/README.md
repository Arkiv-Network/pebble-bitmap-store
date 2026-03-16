# pebblestore

## Key Layout in PebbleDB

The store uses **prefix-namespaced binary keys**. All multi-byte integers are big-endian (BE), which gives correct lexicographic ordering for range scans.

| Prefix | Key Layout | Value | Purpose |
|--------|-----------|-------|---------|
| `0x01` | `[0x01]` | `uint64 BE` | **Last block** — the most recently ingested block number |
| `0x02` | `[0x02][id:8BE]` | encoded payload (see below) | **Payload record** — the full entity data |
| `0x03` | `[0x03][entityKey:32]` | `[id:8BE]` | **Entity-current pointer** — maps a 32-byte entity key to its current (active) payload ID |
| `0x04` | `[0x04][fromBlock:8BE][id:8BE]` | `[toBlock:8BE][toBlockIsNull:1]` | **From-block index** — temporal index for range queries |
| `0x05` | `[0x05][toBlock:8BE][id:8BE]` | empty | **To-block index** — used for pruning closed payloads |
| `0x06` | `[0x06]` | `uint64 BE` | **ID counter** — monotonically increasing, persisted across restarts |
| `0x07` | `[0x07][block:8BE]` | `int64 BE` | **Entity count** — number of active entities at the given block height |
| `0x10` | `[0x10][nameLen:2BE][name][valueLen:2BE][value][block:8BE]` | `[isKeyframe:1][roaring bitmap bytes]` | **String bitmap index** |
| `0x20` | `[0x20][nameLen:2BE][name][value:8BE][block:8BE]` | `[isKeyframe:1][roaring bitmap bytes]` | **Numeric bitmap index** |

## Payload Value Format (`0x02`)

Each payload is a single contiguous byte blob:

```
[fromBlock:8BE][toBlock:8BE][toBlockIsNull:1byte][entityKey:32]
[contentTypeLen:2BE][contentType bytes]
[stringAttrsLen:4BE][stringAttrsJSON bytes]
[numericAttrsLen:4BE][numericAttrsJSON bytes]
[payloadBytes...]
```

- **Active** payloads have `toBlockIsNull=0x01` (toBlock is meaningless).
- **Closed** payloads have `toBlockIsNull=0x00` and `toBlock` set to the block at which they were superseded/deleted.

## Entity Lifecycle

Each blockchain event batch is processed atomically in a single `pebble.IndexedBatch`:

1. **Create** — Allocates a new ID via the `0x06` counter, writes the `0x02` payload, sets the `0x03` entity-current pointer, and writes the `0x04` from-block index. Synthetic attributes (`$owner`, `$creator`, `$key`, `$expiration`, `$sequence`, etc.) are injected.

2. **Update** — Reads the current payload via `0x03` -> `0x02`, closes the old version (`ClosePayloadVersion` sets `toBlock`, deletes `0x03`, writes `0x05`), then inserts a new version. Old attribute bitmap memberships are removed, new ones added.

3. **Delete/Expire** — Removes bitmap memberships for the current version, then closes it.

4. **ExtendBTL / ChangeOwner** — Close-and-reinsert pattern, modifying only the relevant attribute.

At the end of each block, the net entity count change is computed (creates that introduce a new entity increment by 1, deletes/expires decrement by 1, updates and other operations are net zero) and persisted under the `0x07` prefix. This allows `GetNumberOfEntities(ctx, block)` to return the count at any historical block height via a single seek, rather than scanning all `0x03` keys. For pre-upgrade databases without `0x07` entries, the first `FollowEvents` call seeds the counter by scanning `0x03`.

## Bitmap Index

Bitmaps are **Roaring64 bitmaps** where each set bit represents a **payload ID**. For every attribute `(name, value)` pair, there is a chain of bitmap entries keyed by block number.

### Keyframe/delta chain

To avoid storing a full bitmap copy at every block, the store uses a compression scheme:

- Every `KeyframeInterval` blocks (or when a bitmap is first created), a **keyframe** (`isKeyframe=0x01`) stores the complete bitmap.
- Between keyframes, **deltas** (`isKeyframe=0x00`) store only the XOR difference from the previous state.
- Reconstruction walks backward from the end to find the latest keyframe, then forward-applies all deltas via OR/XOR to get the current bitmap.

The `bitmapCache` accumulates in-memory bitmap modifications across an entire event batch, then flushes once — deciding whether to write a keyframe or a delta based on the count of deltas since the last keyframe.

## How Queries Work

Query execution follows this pipeline:

```
Query string  ->  Parse  ->  Evaluate (bitmap ops)  ->  Paginate  ->  Fetch payloads
```

**Step 1: Snapshot isolation.** A `pebble.Snapshot` is taken so all reads see a consistent state.

**Step 2: Parse.** The query string is parsed by the `query` package into an AST.

**Step 3: Evaluate.** The query evaluator walks the AST. At each leaf node, it:

- **Finds matching attribute values** — e.g., for `$owner = "0xabc..."`, it scans all `0x10` keys with the `$owner` name prefix and returns values matching the filter. Supports `=`, `!=`, `<`, `>`, `<=`, `>=`, `GLOB`, `IN`.
- **Reconstructs the bitmap** for each matching `(name, value)` pair at the target block. This finds the latest keyframe at-or-before the target block, then applies deltas up to that block.
- **Combines bitmaps** via set operations (AND / OR / AND NOT) as dictated by the query AST, producing a final Roaring64 bitmap of matching payload IDs.

For "all current entities" queries without attribute filters, `EvaluateAllCurrent` scans the `0x03` prefix. For historical "at block N" queries, `EvaluateAllAtBlock` scans the `0x04` from-block index, filtering by `fromBlock <= N` and `(toBlockIsNull || toBlock > N)`.

**Step 4: Paginate.** The result bitmap is iterated in **reverse** (highest ID first = newest). A cursor-based scheme masks out IDs at or above the cursor value using `bitmap.And(cursorMask)`. Results are limited to `resultsPerPage` (max 200).

**Step 5: Fetch payloads.** The selected IDs are looked up via `RetrievePayloads` (batch `0x02` gets), decoded, and projected based on the `IncludeData` flags (key, payload, attributes, synthetic attributes, etc.).

## Maintenance

- **Pruning** (`PruneBefore`): Deletes closed payloads (`toBlock <= threshold`) and their index entries. For bitmaps, finds the latest keyframe at-or-before the threshold and deletes all entries before it. For entity counts, removes old `0x07` entries before the threshold while keeping the latest one as a base for historical lookups.
- **Reorg handling** (`HandleReorg`): Rolls back to a block by reopening payloads closed after that block, deleting payloads created after it, removing bitmap entries with block > target, and deleting entity count entries after the target block.
