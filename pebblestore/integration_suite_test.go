package pebblestore_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"

	arkivevents "github.com/Arkiv-Network/arkiv-events"
	"github.com/Arkiv-Network/arkiv-events/events"
	"github.com/Arkiv-Network/pebble-bitmap-store/pebblestore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "PebbleStore Integration Suite")
}

// operation is a type alias so test files can reference the type without
// importing the events package directly.
type operation = events.Operation

var walletAddress = common.HexToAddress("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")

func newStore() *pebblestore.PebbleStore {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := pebblestore.NewPebbleStore(logger, "")
	Expect(err).NotTo(HaveOccurred())
	return s
}

// processBlock feeds a single block of operations into the store via FollowEvents.
func processBlock(ctx context.Context, s *pebblestore.PebbleStore, blockNumber uint64, ops []operation) {
	batch := events.BlockBatch{
		Blocks: []events.Block{
			{
				Number:     blockNumber,
				Operations: ops,
			},
		},
	}
	err := s.FollowEvents(ctx, func(yield func(arkivevents.BatchOrError) bool) {
		yield(arkivevents.BatchOrError{Batch: batch})
	})
	Expect(err).NotTo(HaveOccurred())
}

func createOp(key common.Hash, owner common.Address, payload []byte, btl uint64, stringAttrs map[string]string, numericAttrs map[string]uint64, txIndex, opIndex uint64) operation {
	return operation{
		TxIndex: txIndex,
		OpIndex: opIndex,
		Create: &events.OPCreate{
			Key:               key,
			ContentType:       "application/octet-stream",
			BTL:               btl,
			Owner:             owner,
			Content:           payload,
			StringAttributes:  stringAttrs,
			NumericAttributes: numericAttrs,
		},
	}
}

func updateOp(key common.Hash, owner common.Address, payload []byte, btl uint64, stringAttrs map[string]string, numericAttrs map[string]uint64, txIndex, opIndex uint64) operation {
	return operation{
		TxIndex: txIndex,
		OpIndex: opIndex,
		Update: &events.OPUpdate{
			Key:               key,
			ContentType:       "application/octet-stream",
			BTL:               btl,
			Owner:             owner,
			Content:           payload,
			StringAttributes:  stringAttrs,
			NumericAttributes: numericAttrs,
		},
	}
}

func deleteOp(key common.Hash, txIndex, opIndex uint64) operation {
	del := events.OPDelete(key)
	return operation{
		TxIndex: txIndex,
		OpIndex: opIndex,
		Delete:  &del,
	}
}

// testEntityData mirrors the subset of entity fields compared in the original
// stress tests (Key, Value, ExpiresAt, Owner, user-defined attributes).
type testEntityData struct {
	Key               *common.Hash
	Value             []byte
	ExpiresAt         *uint64
	Owner             *common.Address
	StringAttributes  map[string]string
	NumericAttributes map[string]uint64
}

func parseQueryResults(resp *pebblestore.QueryResponse) []pebblestore.EntityData {
	results := make([]pebblestore.EntityData, 0, len(resp.Data))
	for _, raw := range resp.Data {
		var ed pebblestore.EntityData
		err := json.Unmarshal(raw, &ed)
		Expect(err).NotTo(HaveOccurred())
		results = append(results, ed)
	}
	return results
}

// toTestEntity converts a pebblestore.EntityData into a testEntityData for
// comparison, extracting only the fields we care about and converting
// attribute slices to maps.
func toTestEntity(ed *pebblestore.EntityData) *testEntityData {
	te := &testEntityData{
		Key:               ed.Key,
		Value:             []byte(ed.Value),
		ExpiresAt:         ed.ExpiresAt,
		Owner:             ed.Owner,
		StringAttributes:  make(map[string]string),
		NumericAttributes: make(map[string]uint64),
	}
	for _, sa := range ed.StringAttributes {
		te.StringAttributes[sa.Key] = sa.Value
	}
	for _, na := range ed.NumericAttributes {
		te.NumericAttributes[na.Key] = na.Value
	}
	return te
}

func pointerOf[T any](v T) *T {
	return &v
}

func ownerQuery() string {
	return "$owner=" + strings.ToLower(walletAddress.Hex())
}

func keyQuery(key common.Hash) string {
	return "$key=" + strings.ToLower(key.Hex())
}

func entityKeyFromSeed(seed string) common.Hash {
	return crypto.Keccak256Hash([]byte(seed))
}

type entityVersion struct {
	Data        *testEntityData
	BlockNumber uint64
}
