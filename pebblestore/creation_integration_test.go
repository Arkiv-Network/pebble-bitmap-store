package pebblestore_test

import (
	"context"

	"github.com/Arkiv-Network/pebble-bitmap-store/pebblestore"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Creation", func() {
	var s *pebblestore.PebbleStore
	var blockNum uint64
	var entityVersions []entityVersion

	entityKey := entityKeyFromSeed("creation-test-entity")

	BeforeEach(func() {
		s = newStore()
		blockNum = 0
		entityVersions = nil
	})

	AfterEach(func() {
		s.Close()
	})

	When("I create a new entity", func() {
		BeforeEach(func(ctx context.Context) {
			blockNum++

			entityData := testEntityData{
				Value: []byte("entity-payload-1"),
				Owner: &walletAddress,
				StringAttributes: map[string]string{
					"string_annotation_1_1": "sa_value_1_1",
					"string_annotation_1_2": "sa_value_1_2",
				},
				NumericAttributes: map[string]uint64{
					"numeric_annotation_1_1": 1,
					"numeric_annotation_1_2": 2,
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

		It("should exist", func(ctx context.Context) {
			resp, err := s.QueryEntities(ctx, keyQuery(entityKey), nil)
			Expect(err).NotTo(HaveOccurred())
			results := parseQueryResults(resp)
			Expect(results).To(HaveLen(1))
			Expect(toTestEntity(&results[0])).To(Equal(entityVersions[0].Data))
		})

		It("should be found by string annotation", func(ctx context.Context) {
			resp, err := s.QueryEntities(ctx,
				`string_annotation_1_1="sa_value_1_1" && `+keyQuery(entityKey), nil)
			Expect(err).NotTo(HaveOccurred())
			results := parseQueryResults(resp)
			Expect(results).To(HaveLen(1))
			Expect(toTestEntity(&results[0])).To(Equal(entityVersions[0].Data))
		})

		It("should be found by numeric annotation", func(ctx context.Context) {
			resp, err := s.QueryEntities(ctx,
				`numeric_annotation_1_1=1 && `+keyQuery(entityKey), nil)
			Expect(err).NotTo(HaveOccurred())
			results := parseQueryResults(resp)
			Expect(results).To(HaveLen(1))
			Expect(toTestEntity(&results[0])).To(Equal(entityVersions[0].Data))
		})

		It("should be found when querying only for attributes", func(ctx context.Context) {
			resp, err := s.QueryEntities(ctx, keyQuery(entityKey), &pebblestore.Options{
				IncludeData: &pebblestore.IncludeData{
					Attributes: true,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			results := parseQueryResults(resp)
			Expect(results).To(HaveLen(1))

			ed := &results[0]
			Expect(ed.Key).To(BeNil())
			Expect(ed.Value).To(BeNil())
			Expect(ed.ContentType).To(BeNil())
			Expect(ed.ExpiresAt).To(BeNil())
			Expect(ed.Owner).To(BeNil())
			Expect(ed.Creator).To(BeNil())
			Expect(ed.CreatedAtBlock).To(BeNil())
			Expect(ed.LastModifiedAtBlock).To(BeNil())
			Expect(ed.TransactionIndexInBlock).To(BeNil())
			Expect(ed.OperationIndexInTransaction).To(BeNil())

			Expect(ed.StringAttributes).ToNot(BeEmpty())
			Expect(ed.NumericAttributes).ToNot(BeEmpty())
		})

		It("should be found when querying with no data requested", func(ctx context.Context) {
			resp, err := s.QueryEntities(ctx, keyQuery(entityKey), &pebblestore.Options{
				IncludeData: &pebblestore.IncludeData{},
			})
			Expect(err).NotTo(HaveOccurred())
			results := parseQueryResults(resp)
			Expect(results).To(HaveLen(1))

			ed := &results[0]
			Expect(ed.Key).To(BeNil())
			Expect(ed.Value).To(BeNil())
			Expect(ed.ContentType).To(BeNil())
			Expect(ed.ExpiresAt).To(BeNil())
			Expect(ed.Owner).To(BeNil())
			Expect(ed.CreatedAtBlock).To(BeNil())
			Expect(ed.LastModifiedAtBlock).To(BeNil())
			Expect(ed.TransactionIndexInBlock).To(BeNil())
			Expect(ed.OperationIndexInTransaction).To(BeNil())

			Expect(ed.StringAttributes).To(BeEmpty())
			Expect(ed.NumericAttributes).To(BeEmpty())
		})

		When("I update the entity", func() {
			BeforeEach(func(ctx context.Context) {
				blockNum++

				entityData := testEntityData{
					Key:   &entityKey,
					Value: []byte("entity-payload-2"),
					Owner: &walletAddress,
					StringAttributes: map[string]string{
						"string_annotation_2_1": "sa_value_2_1",
						"string_annotation_2_2": "sa_value_2_2",
					},
					NumericAttributes: map[string]uint64{},
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

			It("should be found by key", func(ctx context.Context) {
				resp, err := s.QueryEntities(ctx, keyQuery(entityKey), nil)
				Expect(err).NotTo(HaveOccurred())
				results := parseQueryResults(resp)
				Expect(results).To(HaveLen(1))
				Expect(toTestEntity(&results[0])).To(Equal(entityVersions[1].Data))
			})

			It("should be found by string annotation", func(ctx context.Context) {
				resp, err := s.QueryEntities(ctx,
					`string_annotation_2_1="sa_value_2_1" && `+keyQuery(entityKey), nil)
				Expect(err).NotTo(HaveOccurred())
				results := parseQueryResults(resp)
				Expect(results).To(HaveLen(1))
				Expect(toTestEntity(&results[0])).To(Equal(entityVersions[1].Data))
			})

			It("should not be found by the old string annotation", func(ctx context.Context) {
				resp, err := s.QueryEntities(ctx,
					`string_annotation_1_1="sa_value_1_1" && `+keyQuery(entityKey), nil)
				Expect(err).NotTo(HaveOccurred())
				results := parseQueryResults(resp)
				Expect(results).To(HaveLen(0))
			})

			When("I update the entity again with a new string annotation", func() {
				BeforeEach(func(ctx context.Context) {
					blockNum++

					entityData := testEntityData{
						Key:   &entityKey,
						Value: []byte("entity-payload-3"),
						Owner: &walletAddress,
						StringAttributes: map[string]string{
							"string_annotation_3_1": "sa_value_3_1",
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

				It("should be found by string annotation", func(ctx context.Context) {
					resp, err := s.QueryEntities(ctx,
						`string_annotation_3_1="sa_value_3_1" && `+keyQuery(entityKey), nil)
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

					It("should not be found by the pre-deletion string annotation", func(ctx context.Context) {
						resp, err := s.QueryEntities(ctx,
							`string_annotation_3_1="sa_value_3_1" && `+keyQuery(entityKey), nil)
						Expect(err).NotTo(HaveOccurred())
						results := parseQueryResults(resp)
						Expect(results).To(HaveLen(0))
					})

					It("should not be found by the initial string annotation", func(ctx context.Context) {
						resp, err := s.QueryEntities(ctx,
							`string_annotation_1_1="sa_value_1_1" && `+keyQuery(entityKey), nil)
						Expect(err).NotTo(HaveOccurred())
						results := parseQueryResults(resp)
						Expect(results).To(HaveLen(0))
					})
				})
			})
		})
	})
})
