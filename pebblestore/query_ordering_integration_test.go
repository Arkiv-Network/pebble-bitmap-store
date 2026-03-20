package pebblestore_test

import (
	"context"
	"fmt"
	"slices"

	"github.com/Arkiv-Network/pebble-bitmap-store/pebblestore"
	"github.com/ethereum/go-ethereum/common"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Query Result Ordering", func() {
	var s *pebblestore.PebbleStore
	var blockNum uint64

	BeforeEach(func() {
		s = newStore()
		blockNum = 0
	})

	AfterEach(func() {
		s.Close()
	})

	When("I create 20 entities in the same transaction", func() {

		var entityIDs []common.Hash

		BeforeEach(func(ctx context.Context) {
			blockNum++
			entityIDs = []common.Hash{}

			ops := make([]operation, 20)
			for i := range 20 {
				key := entityKeyFromSeed(fmt.Sprintf("ordering-same-tx-%d", i))
				entityIDs = append(entityIDs, key)

				ops[i] = createOp(
					key,
					walletAddress,
					[]byte(fmt.Sprintf("entity-payload-%d", i)),
					200000,
					map[string]string{
						fmt.Sprintf("string_annotation_%d_1", i): fmt.Sprintf("sa_value_%d_1", i),
						fmt.Sprintf("string_annotation_%d_2", i): fmt.Sprintf("sa_value_%d_2", i),
					},
					map[string]uint64{
						fmt.Sprintf("numeric_annotation_%d_1", i): uint64(i),
						fmt.Sprintf("numeric_annotation_%d_2", i): uint64(i),
					},
					0,
					uint64(i),
				)
			}

			processBlock(ctx, s, blockNum, ops)

			Expect(entityIDs).To(HaveLen(20))
		})

		When("I query the entities by owner", func() {
			It("should return the entities in the correct order", func(ctx context.Context) {
				resp, err := s.QueryEntities(ctx, ownerQuery(), &pebblestore.Options{
					IncludeData: &pebblestore.IncludeData{
						Key: true,
					},
				})
				Expect(err).NotTo(HaveOccurred())
				results := parseQueryResults(resp)
				Expect(results).To(HaveLen(20))

				reversed := slices.Clone(entityIDs)
				slices.Reverse(reversed)
				for i, entity := range results {
					Expect(reversed[i]).To(Equal(*entity.Key))
				}
			})
		})
	})

	When("I create 20 entities in consecutive transactions", func() {
		var entityIDs []common.Hash

		BeforeEach(func(ctx context.Context) {
			entityIDs = []common.Hash{}

			for i := range 20 {
				blockNum++
				key := entityKeyFromSeed(fmt.Sprintf("ordering-consecutive-%d", i))
				entityIDs = append(entityIDs, key)

				processBlock(ctx, s, blockNum, []operation{
					createOp(
						key,
						walletAddress,
						[]byte(fmt.Sprintf("entity-payload-%d", i)),
						200000,
						map[string]string{
							fmt.Sprintf("string_annotation_%d_1", i): fmt.Sprintf("sa_value_%d_1", i),
							fmt.Sprintf("string_annotation_%d_2", i): fmt.Sprintf("sa_value_%d_2", i),
						},
						map[string]uint64{
							fmt.Sprintf("numeric_annotation_%d_1", i): uint64(i),
							fmt.Sprintf("numeric_annotation_%d_2", i): uint64(i),
						},
						0, 0,
					),
				})
			}

			Expect(entityIDs).To(HaveLen(20))
		})

		When("I query the entities by owner", func() {
			It("should return the entities in the correct order", func(ctx context.Context) {
				resp, err := s.QueryEntities(ctx, ownerQuery(), nil)
				Expect(err).NotTo(HaveOccurred())
				results := parseQueryResults(resp)
				Expect(results).To(HaveLen(20))

				reversed := slices.Clone(entityIDs)
				slices.Reverse(reversed)
				for i, entity := range results {
					Expect(reversed[i]).To(Equal(*entity.Key))
				}
			})
		})
	})
})
