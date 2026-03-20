package pebblestore_test

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/Arkiv-Network/pebble-bitmap-store/pebblestore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pagination", func() {
	var s *pebblestore.PebbleStore
	var blockNum uint64

	BeforeEach(func() {
		s = newStore()
		blockNum = 0
	})

	AfterEach(func() {
		s.Close()
	})

	When("I create 20 entities", func() {

		var entityIDs []common.Hash
		var reverseEntityIDs []common.Hash

		BeforeEach(func(ctx context.Context) {
			blockNum++

			entityIDs = []common.Hash{}

			ops := make([]operation, 20)
			for i := range 20 {
				key := entityKeyFromSeed(fmt.Sprintf("pagination-entity-%d", i))
				entityIDs = append(entityIDs, key)

				ops[i] = createOp(
					key,
					walletAddress,
					[]byte(fmt.Sprintf("entity-payload-%d", i)),
					200000,
					map[string]string{
						fmt.Sprintf("string_annotation_%d_1", i): fmt.Sprintf("sa_value_%d_1", i),
						fmt.Sprintf("string_annotation_%d_2", i): fmt.Sprintf("sa_value_%d_2", i),
						"revindex":                               strings.Repeat("a", (20 - i)),
					},
					map[string]uint64{
						fmt.Sprintf("numeric_annotation_%d_1", i): uint64(i),
						fmt.Sprintf("numeric_annotation_%d_2", i): uint64(i),
						"revindex": uint64(20 - i),
					},
					0,
					uint64(i),
				)
			}

			processBlock(ctx, s, blockNum, ops)

			reverseEntityIDs = slices.Clone(entityIDs)
			slices.Reverse(reverseEntityIDs)

			Expect(entityIDs).To(HaveLen(20))
			Expect(reverseEntityIDs).To(HaveLen(20))
		})

		When("I query the entities by owner, sorted by $sequence, descending, and request page of 5 entities", func() {
			var resp *pebblestore.QueryResponse
			var cursor string

			BeforeEach(func(ctx context.Context) {
				var err error
				resp, err = s.QueryEntities(ctx, ownerQuery(), &pebblestore.Options{
					ResultsPerPage: pointerOf(hexutil.Uint64(5)),
					IncludeData: &pebblestore.IncludeData{
						Key: true,
					},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Cursor).ToNot(BeNil(), "num of res", len(resp.Data))
				cursor = *resp.Cursor
			})

			It("should return the entities in the correct order", func(ctx context.Context) {
				results := parseQueryResults(resp)
				Expect(results).To(HaveLen(5))
				Expect(resp.BlockNumber).ToNot(Equal(uint64(0)))

				Expect(results[0].Owner).To(BeNil())
				Expect(results[0].ExpiresAt).To(BeNil())
				Expect(results[0].Value).To(BeNil())
				Expect(results[0].StringAttributes).To(BeNil())
				Expect(results[0].NumericAttributes).To(BeNil())

				ids := []common.Hash{}
				for _, entity := range results {
					ids = append(ids, *entity.Key)
				}
				Expect(ids).To(Equal(reverseEntityIDs[:5]))
			})

			When("I request the next page", func() {

				BeforeEach(func(ctx context.Context) {
					var err error
					resp, err = s.QueryEntities(ctx, ownerQuery(), &pebblestore.Options{
						ResultsPerPage: pointerOf(hexutil.Uint64(5)),
						IncludeData: &pebblestore.IncludeData{
							Key: true,
						},
						Cursor: cursor,
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(resp.Cursor).ToNot(BeNil(), "num of res", len(resp.Data))
					cursor = *resp.Cursor
				})

				It("should return the next page of entities in the correct order", func(ctx context.Context) {
					results := parseQueryResults(resp)
					Expect(results).To(HaveLen(5))
					Expect(resp.BlockNumber).ToNot(Equal(uint64(0)))

					ids := []common.Hash{}
					for _, entity := range results {
						ids = append(ids, *entity.Key)
					}
					Expect(ids).To(Equal(reverseEntityIDs[5:10]))
				})

				When("I request the last", func() {

					BeforeEach(func(ctx context.Context) {
						var err error
						resp, err = s.QueryEntities(ctx, ownerQuery(), &pebblestore.Options{
							IncludeData: &pebblestore.IncludeData{
								Key: true,
							},
							Cursor: cursor,
						})
						Expect(err).NotTo(HaveOccurred())
					})

					It("should return the next page of entities in the correct order", func(ctx context.Context) {
						results := parseQueryResults(resp)
						Expect(results).To(HaveLen(10))
						Expect(resp.BlockNumber).ToNot(Equal(uint64(0)))
						Expect(resp.Cursor).To(BeNil())

						ids := []common.Hash{}
						for _, entity := range results {
							ids = append(ids, *entity.Key)
						}
						Expect(ids).To(Equal(reverseEntityIDs[10:20]))
					})

				})

			})

		})

	})

})
