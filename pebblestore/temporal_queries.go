package pebblestore

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble"
)

// EvaluateAllCurrent returns the IDs of all entities that have a current
// (active) payload, sorted by ID descending. It scans all keys with the
// entity-current prefix (0x03).
func (s *PebbleStore) EvaluateAllCurrent(ctx context.Context, reader pebble.Reader) ([]uint64, error) {
	lower := []byte{prefixEntityCurrent}
	upper := []byte{prefixEntityCurrent + 1}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("pebblestore: create iterator: %w", err)
	}
	defer iter.Close()

	var ids []uint64
	for iter.First(); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("pebblestore: read value: %w", err)
		}
		id := binary.BigEndian.Uint64(val)
		ids = append(ids, id)
	}

	// Sort descending to match SQLite's ORDER BY id DESC.
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] > ids[j]
	})

	return ids, nil
}

// EvaluateAllAtBlock returns the IDs of all entities that were active at the
// given block number, sorted by ID descending. An entity is considered active
// at a block if its fromBlock <= block and either its toBlock is null (still
// active) or its toBlock > block.
func (s *PebbleStore) EvaluateAllAtBlock(ctx context.Context, reader pebble.Reader, block uint64) ([]uint64, error) {
	lower := fromBlockIndexKey(0, 0)
	upper := fromBlockIndexKey(block+1, 0)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("pebblestore: create iterator: %w", err)
	}
	defer iter.Close()

	var ids []uint64
	for iter.First(); iter.Valid(); iter.Next() {
		_, id := parseFromBlockIndexKey(iter.Key())

		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("pebblestore: read value: %w", err)
		}

		// Value layout: [toBlock:8BE][toBlockIsNull:1]
		toBlock := binary.BigEndian.Uint64(val[:8])
		toBlockIsNull := val[8] == 0x01

		if toBlockIsNull || toBlock > block {
			ids = append(ids, id)
		}
	}

	// Sort descending to match SQLite's ORDER BY id DESC.
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] > ids[j]
	})

	return ids, nil
}

// GetNumberOfEntities returns the number of active entities at the given block
// height. Pass block=0 for the latest block. The count is retrieved via a
// single seek on the persisted per-block entity count (0x07 prefix).
func (s *PebbleStore) GetNumberOfEntities(ctx context.Context, block uint64) (int64, error) {
	snap := s.db.NewSnapshot()
	defer snap.Close()

	if block == 0 {
		count, _, found, err := s.readLatestEntityCount(snap)
		if err != nil {
			return 0, err
		}
		if found {
			return count, nil
		}
		// Fallback for pre-upgrade DBs: scan 0x03 keys.
		return s.countEntityCurrentKeys(snap)
	}

	return s.readEntityCountAtBlock(snap, block)
}

// readLatestEntityCount seeks to the last 0x07 entry and returns its count
// and block number. found is false when no entries exist.
func (s *PebbleStore) readLatestEntityCount(reader pebble.Reader) (count int64, block uint64, found bool, err error) {
	lower := []byte{prefixEntityCount}
	upper := []byte{prefixEntityCount + 1}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return 0, 0, false, fmt.Errorf("pebblestore: create entity count iterator: %w", err)
	}
	defer iter.Close()

	if !iter.Last() {
		return 0, 0, false, iter.Error()
	}

	val, err := iter.ValueAndErr()
	if err != nil {
		return 0, 0, false, fmt.Errorf("pebblestore: read entity count value: %w", err)
	}

	count = int64(binary.BigEndian.Uint64(val))
	block = binary.BigEndian.Uint64(iter.Key()[1:])
	return count, block, true, nil
}

// readEntityCountAtBlock finds the entity count at or before the given block.
func (s *PebbleStore) readEntityCountAtBlock(reader pebble.Reader, block uint64) (int64, error) {
	lower := []byte{prefixEntityCount}
	upper := []byte{prefixEntityCount + 1}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return 0, fmt.Errorf("pebblestore: create entity count iterator: %w", err)
	}
	defer iter.Close()

	// Seek to block+1 then step back to find the entry at or before block.
	target := entityCountKey(block + 1)
	iter.SeekLT(target)
	if !iter.Valid() {
		// No entry at or before this block — zero entities.
		return 0, iter.Error()
	}

	val, err := iter.ValueAndErr()
	if err != nil {
		return 0, fmt.Errorf("pebblestore: read entity count value: %w", err)
	}

	return int64(binary.BigEndian.Uint64(val)), nil
}

// countEntityCurrentKeys counts active entities by scanning 0x03 keys.
// Used as a one-time fallback for databases that predate per-block tracking.
func (s *PebbleStore) countEntityCurrentKeys(reader pebble.Reader) (int64, error) {
	lower := []byte{prefixEntityCurrent}
	upper := []byte{prefixEntityCurrent + 1}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return 0, fmt.Errorf("pebblestore: create iterator: %w", err)
	}
	defer iter.Close()

	var count int64
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	return count, nil
}
