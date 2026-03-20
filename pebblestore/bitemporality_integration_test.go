package pebblestore_test

import (
	"context"

	"github.com/Arkiv-Network/pebble-bitmap-store/pebblestore"
	"github.com/ethereum/go-ethereum/common/hexutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bitemporality", func() {
	var s *pebblestore.PebbleStore
	var blockNum uint64
	var entityVersions []entityVersion

	entityKey := entityKeyFromSeed("bitemporality-test-entity")

	BeforeEach(func() {
		s = newStore()
		blockNum = 0
		entityVersions = nil
	})

	AfterEach(func() {
		s.Close()
	})

	When("I create an entity", func() {
		BeforeEach(func(ctx context.Context) {
			blockNum++

			entityData := testEntityData{
				Value: []byte("bitemporal-payload-1"),
				Owner: &walletAddress,
				StringAttributes: map[string]string{
					"bitemporal_str_1_1": "bt_value_1_1",
					"bitemporal_str_1_2": "bt_value_1_2",
				},
				NumericAttributes: map[string]uint64{
					"bitemporal_num_1_1": 1,
					"bitemporal_num_1_2": 2,
				},
			}

			processBlock(ctx, s, blockNum, []operation{
				createOp(entityKey, walletAddress, entityData.Value, 200000,
					entityData.StringAttributes, entityData.NumericAttributes, 0, 0),
			})

			entityData.Key = &entityKey
			expiry := blockNum + 200000
			entityData.ExpiresAt = &expiry

			entityVersions = []entityVersion{
				{
					Data:        &entityData,
					BlockNumber: blockNum,
				},
			}
		})

		It("should be queryable at the creation block", func(ctx context.Context) {
			atBlock := entityVersions[0].BlockNumber
			resp, err := s.QueryEntities(ctx, keyQuery(entityKey), &pebblestore.Options{
				AtBlock: pointerOf(hexutil.Uint64(atBlock)),
			})
			Expect(err).NotTo(HaveOccurred())
			results := parseQueryResults(resp)
			Expect(results).To(HaveLen(1))
			Expect(toTestEntity(&results[0])).To(Equal(entityVersions[0].Data))
		})

		When("I update the entity", func() {
			BeforeEach(func(ctx context.Context) {
				blockNum++

				entityData := testEntityData{
					Key:   &entityKey,
					Value: []byte("bitemporal-payload-2"),
					Owner: &walletAddress,
					StringAttributes: map[string]string{
						"bitemporal_str_2_1": "bt_value_2_1",
						"bitemporal_str_2_2": "bt_value_2_2",
					},
					NumericAttributes: map[string]uint64{
						"bitemporal_num_2_1": 10,
					},
				}

				processBlock(ctx, s, blockNum, []operation{
					updateOp(entityKey, walletAddress, entityData.Value, 300000,
						entityData.StringAttributes, entityData.NumericAttributes, 0, 0),
				})

				expiry := blockNum + 300000
				entityData.ExpiresAt = &expiry

				entityVersions = append(entityVersions, entityVersion{
					Data:        &entityData,
					BlockNumber: blockNum,
				})
			})

			It("at creation block, should return original data by key", func(ctx context.Context) {
				atBlock := entityVersions[0].BlockNumber
				resp, err := s.QueryEntities(ctx, keyQuery(entityKey), &pebblestore.Options{
					AtBlock: pointerOf(hexutil.Uint64(atBlock)),
				})
				Expect(err).NotTo(HaveOccurred())
				results := parseQueryResults(resp)
				Expect(results).To(HaveLen(1))
				Expect(toTestEntity(&results[0])).To(Equal(entityVersions[0].Data))
			})

			It("at creation block, should return original data by old string annotation", func(ctx context.Context) {
				atBlock := entityVersions[0].BlockNumber
				resp, err := s.QueryEntities(ctx,
					`bitemporal_str_1_1="bt_value_1_1" && `+keyQuery(entityKey),
					&pebblestore.Options{
						AtBlock: pointerOf(hexutil.Uint64(atBlock)),
					})
				Expect(err).NotTo(HaveOccurred())
				results := parseQueryResults(resp)
				Expect(results).To(HaveLen(1))
				Expect(toTestEntity(&results[0])).To(Equal(entityVersions[0].Data))
			})

			It("at creation block, should NOT find entity by new string annotation", func(ctx context.Context) {
				atBlock := entityVersions[0].BlockNumber
				resp, err := s.QueryEntities(ctx,
					`bitemporal_str_2_1="bt_value_2_1" && `+keyQuery(entityKey),
					&pebblestore.Options{
						AtBlock: pointerOf(hexutil.Uint64(atBlock)),
					})
				Expect(err).NotTo(HaveOccurred())
				results := parseQueryResults(resp)
				Expect(results).To(HaveLen(0))
			})

			It("at current block, should return updated data", func(ctx context.Context) {
				resp, err := s.QueryEntities(ctx, keyQuery(entityKey), nil)
				Expect(err).NotTo(HaveOccurred())
				results := parseQueryResults(resp)
				Expect(results).To(HaveLen(1))
				Expect(toTestEntity(&results[0])).To(Equal(entityVersions[1].Data))
			})

			When("I update the entity again", func() {
				BeforeEach(func(ctx context.Context) {
					blockNum++

					entityData := testEntityData{
						Key:   &entityKey,
						Value: []byte("bitemporal-payload-3"),
						Owner: &walletAddress,
						StringAttributes: map[string]string{
							"bitemporal_str_3_1": "bt_value_3_1",
						},
						NumericAttributes: map[string]uint64{},
					}

					processBlock(ctx, s, blockNum, []operation{
						updateOp(entityKey, walletAddress, entityData.Value, 400000,
							entityData.StringAttributes, entityData.NumericAttributes, 0, 0),
					})

					expiry := blockNum + 400000
					entityData.ExpiresAt = &expiry

					entityVersions = append(entityVersions, entityVersion{
						Data:        &entityData,
						BlockNumber: blockNum,
					})
				})

				It("at creation block, should return original data", func(ctx context.Context) {
					atBlock := entityVersions[0].BlockNumber
					resp, err := s.QueryEntities(ctx, keyQuery(entityKey), &pebblestore.Options{
						AtBlock: pointerOf(hexutil.Uint64(atBlock)),
					})
					Expect(err).NotTo(HaveOccurred())
					results := parseQueryResults(resp)
					Expect(results).To(HaveLen(1))
					Expect(toTestEntity(&results[0])).To(Equal(entityVersions[0].Data))
				})

				It("at first update block, should return first update data", func(ctx context.Context) {
					atBlock := entityVersions[1].BlockNumber
					resp, err := s.QueryEntities(ctx, keyQuery(entityKey), &pebblestore.Options{
						AtBlock: pointerOf(hexutil.Uint64(atBlock)),
					})
					Expect(err).NotTo(HaveOccurred())
					results := parseQueryResults(resp)
					Expect(results).To(HaveLen(1))
					Expect(toTestEntity(&results[0])).To(Equal(entityVersions[1].Data))
				})

				It("at current block, should return second update data", func(ctx context.Context) {
					resp, err := s.QueryEntities(ctx, keyQuery(entityKey), nil)
					Expect(err).NotTo(HaveOccurred())
					results := parseQueryResults(resp)
					Expect(results).To(HaveLen(1))
					Expect(toTestEntity(&results[0])).To(Equal(entityVersions[2].Data))
				})

				When("I delete the entity", func() {
					BeforeEach(func(ctx context.Context) {
						blockNum++
						processBlock(ctx, s, blockNum, []operation{
							deleteOp(entityKey, 0, 0),
						})
						entityVersions = append(entityVersions, entityVersion{
							Data:        nil,
							BlockNumber: blockNum,
						})
					})

					It("at creation block, should return original data by key", func(ctx context.Context) {
						atBlock := entityVersions[0].BlockNumber
						resp, err := s.QueryEntities(ctx, keyQuery(entityKey), &pebblestore.Options{
							AtBlock: pointerOf(hexutil.Uint64(atBlock)),
						})
						Expect(err).NotTo(HaveOccurred())
						results := parseQueryResults(resp)
						Expect(results).To(HaveLen(1))
						Expect(toTestEntity(&results[0])).To(Equal(entityVersions[0].Data))
					})

					It("at last update block, should return last update data by key", func(ctx context.Context) {
						atBlock := entityVersions[2].BlockNumber
						resp, err := s.QueryEntities(ctx, keyQuery(entityKey), &pebblestore.Options{
							AtBlock: pointerOf(hexutil.Uint64(atBlock)),
						})
						Expect(err).NotTo(HaveOccurred())
						results := parseQueryResults(resp)
						Expect(results).To(HaveLen(1))
						Expect(toTestEntity(&results[0])).To(Equal(entityVersions[2].Data))
					})

					It("at last update block, should return last update data by string annotation", func(ctx context.Context) {
						atBlock := entityVersions[2].BlockNumber
						resp, err := s.QueryEntities(ctx,
							`bitemporal_str_3_1="bt_value_3_1" && `+keyQuery(entityKey),
							&pebblestore.Options{
								AtBlock: pointerOf(hexutil.Uint64(atBlock)),
							})
						Expect(err).NotTo(HaveOccurred())
						results := parseQueryResults(resp)
						Expect(results).To(HaveLen(1))
						Expect(toTestEntity(&results[0])).To(Equal(entityVersions[2].Data))
					})

					It("at current block, should NOT find entity by key", func(ctx context.Context) {
						resp, err := s.QueryEntities(ctx, keyQuery(entityKey), nil)
						Expect(err).NotTo(HaveOccurred())
						results := parseQueryResults(resp)
						Expect(results).To(HaveLen(0))
					})

					It("at current block, should NOT find entity by any annotation", func(ctx context.Context) {
						resp, err := s.QueryEntities(ctx,
							`bitemporal_str_3_1="bt_value_3_1" && `+keyQuery(entityKey), nil)
						Expect(err).NotTo(HaveOccurred())
						results := parseQueryResults(resp)
						Expect(results).To(HaveLen(0))
					})
				})
			})
		})
	})
})
