/*
Copyright (c) 2025 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
*/

package servers

import (
	"context"
	"fmt"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/osac-project/osac/fulfillment-service/internal/database"
	"github.com/osac-project/osac/fulfillment-service/internal/uuid"
	privatev1 "github.com/osac-project/osac/proto/gen/osac/private/v1"
	publicv1 "github.com/osac-project/osac/proto/gen/osac/public/v1"
)

var _ = Describe("Add-on operators server", func() {
	Describe("Creation", func() {
		It("Can be built if all the required parameters are set", func() {
			server, err := NewAddOnOperatorsServer().
				SetLogger(logger).
				SetAttributionLogic(attribution).
				SetTenancyLogic(tenancy).
				Build()
			Expect(err).ToNot(HaveOccurred())
			Expect(server).ToNot(BeNil())
		})

		It("Fails if logger is not set", func() {
			server, err := NewAddOnOperatorsServer().
				SetAttributionLogic(attribution).
				SetTenancyLogic(tenancy).
				Build()
			Expect(err).To(MatchError("logger is mandatory"))
			Expect(server).To(BeNil())
		})

		It("Fails if tenancy logic is not set", func() {
			server, err := NewAddOnOperatorsServer().
				SetLogger(logger).
				SetAttributionLogic(attribution).
				Build()
			Expect(err).To(MatchError("tenancy logic is mandatory"))
			Expect(server).To(BeNil())
		})
	})

	Describe("Published filtering", func() {
		var publicServer *AddOnOperatorsServer
		var privateServer *PrivateAddOnOperatorsServer

		BeforeEach(func() {
			var err error

			privateServer, err = NewPrivateAddOnOperatorsServer().
				SetLogger(logger).
				SetAttributionLogic(attribution).
				SetTenancyLogic(tenancy).
				Build()
			Expect(err).ToNot(HaveOccurred())

			publicServer, err = NewAddOnOperatorsServer().
				SetLogger(logger).
				SetAttributionLogic(attribution).
				SetTenancyLogic(tenancy).
				Build()
			Expect(err).ToNot(HaveOccurred())
		})

		It("List returns only published operators", func() {
			_, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata: privatev1.Metadata_builder{
						Name: fmt.Sprintf("published-%s", uuid.New()[24:32]),
					}.Build(),
					Title:     "Published Operator",
					Published: new(true),
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())

			_, err = privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata: privatev1.Metadata_builder{
						Name: fmt.Sprintf("unpublished-%s", uuid.New()[24:32]),
					}.Build(),
					Title: "Unpublished Operator",
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())

			response, err := publicServer.List(ctx, publicv1.AddOnOperatorsListRequest_builder{}.Build())
			Expect(err).ToNot(HaveOccurred())
			titles := make([]string, len(response.GetItems()))
			for i, item := range response.GetItems() {
				Expect(item.GetPublished()).To(BeTrue())
				titles[i] = item.GetTitle()
			}
			Expect(titles).To(ContainElement("Published Operator"))
			Expect(titles).ToNot(ContainElement("Unpublished Operator"))
		})

		It("Get returns published operator", func() {
			createResponse, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata: privatev1.Metadata_builder{
						Name: fmt.Sprintf("published-%s", uuid.New()[24:32]),
					}.Build(),
					Title:     "Published Operator",
					Published: new(true),
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())

			getResponse, err := publicServer.Get(ctx, publicv1.AddOnOperatorsGetRequest_builder{
				Id: createResponse.GetObject().GetId(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())
			Expect(getResponse.GetObject().GetTitle()).To(Equal("Published Operator"))
		})

		It("Get returns NotFound for unpublished operator", func() {
			createResponse, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata: privatev1.Metadata_builder{
						Name: fmt.Sprintf("unpublished-%s", uuid.New()[24:32]),
					}.Build(),
					Title: "Unpublished Operator",
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())

			_, err = publicServer.Get(ctx, publicv1.AddOnOperatorsGetRequest_builder{
				Id: createResponse.GetObject().GetId(),
			}.Build())
			Expect(err).To(HaveOccurred())
			status, ok := grpcstatus.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(status.Code()).To(Equal(grpccodes.NotFound))
		})

		It("List with filter composes correctly with published filter", func() {
			_, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata: privatev1.Metadata_builder{
						Name: fmt.Sprintf("gpu-%s", uuid.New()[24:32]),
					}.Build(),
					Title:     "GPU Operator",
					Published: new(true),
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())

			response, err := publicServer.List(ctx, publicv1.AddOnOperatorsListRequest_builder{
				Filter: new("this.title == 'GPU Operator'"),
			}.Build())
			Expect(err).ToNot(HaveOccurred())
			Expect(response.GetItems()).ToNot(BeEmpty())
			for _, item := range response.GetItems() {
				Expect(item.GetTitle()).To(Equal("GPU Operator"))
				Expect(item.GetPublished()).To(BeTrue())
			}
		})
	})

	Describe("Dependency persistence", func() {
		var privateServer *PrivateAddOnOperatorsServer

		BeforeEach(func() {
			var err error
			privateServer, err = NewPrivateAddOnOperatorsServer().
				SetLogger(logger).
				SetAttributionLogic(attribution).
				SetTenancyLogic(tenancy).
				Build()
			Expect(err).ToNot(HaveOccurred())
		})

		It("persists canonical direct edges for a published operator", func() {
			dependency, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata:  privatev1.Metadata_builder{Name: "dependency"}.Build(),
					Title:     "dependency",
					Published: new(true),
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())

			dependent, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata:  privatev1.Metadata_builder{Name: "dependent"}.Build(),
					Title:     "dependent",
					Published: new(true),
					Dependencies: []*privatev1.AddOnOperatorLocalReference{
						privatev1.AddOnOperatorLocalReference_builder{Id: dependency.GetObject().GetId()}.Build(),
					},
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())

			var count int
			err = suiteTx.QueryRow(ctx, `
				select count(*)
				from add_on_operator_dependencies
				where operator_id = $1 and dependency_id = $2`,
				dependent.GetObject().GetId(), dependency.GetObject().GetId()).Scan(&count)
			Expect(err).ToNot(HaveOccurred())
			Expect(count).To(Equal(1))
		})

		It("rejects publication when a dependency is not published", func() {
			dependency, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata: privatev1.Metadata_builder{Name: "draft-dependency"}.Build(),
					Title:    "draft-dependency",
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())

			_, err = privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata:  privatev1.Metadata_builder{Name: "invalid-dependent"}.Build(),
					Title:     "invalid-dependent",
					Published: new(true),
					Dependencies: []*privatev1.AddOnOperatorLocalReference{
						privatev1.AddOnOperatorLocalReference_builder{Id: dependency.GetObject().GetId()}.Build(),
					},
				}.Build(),
			}.Build())
			Expect(err).To(HaveOccurred())

			_, err = privateServer.Get(ctx, privatev1.AddOnOperatorsGetRequest_builder{
				Id: "invalid-dependent",
			}.Build())
			Expect(err).To(HaveOccurred())
		})

		It("does not persist dependency edges for draft operators", func() {
			dependency, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata:  privatev1.Metadata_builder{Name: "published-dependency"}.Build(),
					Title:     "published-dependency",
					Published: new(true),
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())

			draft, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata: privatev1.Metadata_builder{Name: "draft-dependent"}.Build(),
					Title:    "draft-dependent",
					Dependencies: []*privatev1.AddOnOperatorLocalReference{
						privatev1.AddOnOperatorLocalReference_builder{Id: dependency.GetObject().GetId()}.Build(),
					},
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())

			var count int
			err = suiteTx.QueryRow(ctx, `
				select count(*)
				from add_on_operator_dependencies
				where operator_id = $1`, draft.GetObject().GetId()).Scan(&count)
			Expect(err).ToNot(HaveOccurred())
			Expect(count).To(Equal(0))
		})

		It("replaces direct edges when published dependencies are updated", func() {
			first, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata:  privatev1.Metadata_builder{Name: "first"}.Build(),
					Title:     "first",
					Published: new(true),
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())
			second, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata:  privatev1.Metadata_builder{Name: "second"}.Build(),
					Title:     "second",
					Published: new(true),
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())
			dependent, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata:  privatev1.Metadata_builder{Name: "updatable"}.Build(),
					Title:     "updatable",
					Published: new(true),
					Dependencies: []*privatev1.AddOnOperatorLocalReference{
						privatev1.AddOnOperatorLocalReference_builder{Id: first.GetObject().GetId()}.Build(),
					},
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())

			_, err = privateServer.Update(ctx, privatev1.AddOnOperatorsUpdateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Id: dependent.GetObject().GetId(),
					Dependencies: []*privatev1.AddOnOperatorLocalReference{
						privatev1.AddOnOperatorLocalReference_builder{Id: second.GetObject().GetId()}.Build(),
					},
				}.Build(),
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"dependencies"}},
			}.Build())
			Expect(err).ToNot(HaveOccurred())

			var dependencyID string
			err = suiteTx.QueryRow(ctx, `
				select dependency_id
				from add_on_operator_dependencies
				where operator_id = $1`, dependent.GetObject().GetId()).Scan(&dependencyID)
			Expect(err).ToNot(HaveOccurred())
			Expect(dependencyID).To(Equal(second.GetObject().GetId()))
		})

		It("rolls back an invalid published dependency update", func() {
			valid, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata:  privatev1.Metadata_builder{Name: "valid"}.Build(),
					Title:     "valid",
					Published: new(true),
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())
			draft, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata: privatev1.Metadata_builder{Name: "unpublished"}.Build(),
					Title:    "unpublished",
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())
			dependent, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata:  privatev1.Metadata_builder{Name: "rollback"}.Build(),
					Title:     "rollback",
					Published: new(true),
					Dependencies: []*privatev1.AddOnOperatorLocalReference{
						privatev1.AddOnOperatorLocalReference_builder{Id: valid.GetObject().GetId()}.Build(),
					},
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())

			_, err = privateServer.Update(ctx, privatev1.AddOnOperatorsUpdateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Id: dependent.GetObject().GetId(),
					Dependencies: []*privatev1.AddOnOperatorLocalReference{
						privatev1.AddOnOperatorLocalReference_builder{Id: draft.GetObject().GetId()}.Build(),
					},
				}.Build(),
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"dependencies"}},
			}.Build())
			Expect(err).To(HaveOccurred())

			var dependencyID string
			err = suiteTx.QueryRow(ctx, `
				select dependency_id
				from add_on_operator_dependencies
				where operator_id = $1`, dependent.GetObject().GetId()).Scan(&dependencyID)
			Expect(err).ToNot(HaveOccurred())
			Expect(dependencyID).To(Equal(valid.GetObject().GetId()))
		})

		It("rejects a published dependency update that invalidates a published dependent", func() {
			dependency, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata:  privatev1.Metadata_builder{Name: "shared-dependency"}.Build(),
					Title:     "shared-dependency",
					Published: new(true),
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())
			dependent, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata:  privatev1.Metadata_builder{Name: "published-dependent"}.Build(),
					Title:     "published-dependent",
					Published: new(true),
					Dependencies: []*privatev1.AddOnOperatorLocalReference{
						privatev1.AddOnOperatorLocalReference_builder{Id: dependency.GetObject().GetId()}.Build(),
					},
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())

			dependencyIDs := make([]string, 0, maxClusterAddOnOperators-1)
			for i := range maxClusterAddOnOperators - 1 {
				leaf, createErr := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
					Object: privatev1.AddOnOperator_builder{
						Metadata:  privatev1.Metadata_builder{Name: fmt.Sprintf("shared-leaf-%d", i)}.Build(),
						Title:     fmt.Sprintf("shared-leaf-%d", i),
						Published: new(true),
					}.Build(),
				}.Build())
				Expect(createErr).ToNot(HaveOccurred())
				dependencyIDs = append(dependencyIDs, leaf.GetObject().GetId())
			}
			_, err = privateServer.Update(ctx, privatev1.AddOnOperatorsUpdateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Id: dependency.GetObject().GetId(),
					Dependencies: func() []*privatev1.AddOnOperatorLocalReference {
						dependencies := make([]*privatev1.AddOnOperatorLocalReference, 0, len(dependencyIDs))
						for _, dependencyID := range dependencyIDs {
							dependencies = append(dependencies,
								privatev1.AddOnOperatorLocalReference_builder{Id: dependencyID}.Build())
						}
						return dependencies
					}(),
				}.Build(),
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"dependencies"}},
			}.Build())
			Expect(err).To(HaveOccurred())

			var dependencyCount int
			err = suiteTx.QueryRow(ctx, `
				select count(*)
				from add_on_operator_dependencies
				where operator_id = $1`, dependency.GetObject().GetId()).Scan(&dependencyCount)
			Expect(err).ToNot(HaveOccurred())
			Expect(dependencyCount).To(Equal(0))
			Expect(dependent.GetObject().GetDependencies()).To(HaveLen(1))
		})

		It("rejects a published dependency update with too many affected dependents", func() {
			dependency, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata:  privatev1.Metadata_builder{Name: "fanout-dependency"}.Build(),
					Title:     "fanout-dependency",
					Published: new(true),
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())
			replacement, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata:  privatev1.Metadata_builder{Name: "fanout-replacement"}.Build(),
					Title:     "fanout-replacement",
					Published: new(true),
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())

			const dependentCount = 257
			for i := range dependentCount {
				_, createErr := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
					Object: privatev1.AddOnOperator_builder{
						Metadata:  privatev1.Metadata_builder{Name: fmt.Sprintf("fanout-dependent-%d", i)}.Build(),
						Title:     fmt.Sprintf("fanout-dependent-%d", i),
						Published: new(true),
						Dependencies: []*privatev1.AddOnOperatorLocalReference{
							privatev1.AddOnOperatorLocalReference_builder{Id: dependency.GetObject().GetId()}.Build(),
						},
					}.Build(),
				}.Build())
				Expect(createErr).ToNot(HaveOccurred())
			}

			_, err = privateServer.Update(ctx, privatev1.AddOnOperatorsUpdateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Id: dependency.GetObject().GetId(),
					Dependencies: []*privatev1.AddOnOperatorLocalReference{
						privatev1.AddOnOperatorLocalReference_builder{Id: replacement.GetObject().GetId()}.Build(),
					},
				}.Build(),
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"dependencies"}},
			}.Build())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("256 affected dependents"))

			var dependencyCountPersisted int
			err = suiteTx.QueryRow(ctx, `
				select count(*)
				from add_on_operator_dependencies
				where operator_id = $1`, dependency.GetObject().GetId()).Scan(&dependencyCountPersisted)
			Expect(err).ToNot(HaveOccurred())
			Expect(dependencyCountPersisted).To(Equal(0))
		})

		It("serializes concurrent published dependency updates", func() {
			first, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata:  privatev1.Metadata_builder{Name: "concurrent-first"}.Build(),
					Title:     "concurrent-first",
					Published: new(true),
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())
			second, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata:  privatev1.Metadata_builder{Name: "concurrent-second"}.Build(),
					Title:     "concurrent-second",
					Published: new(true),
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())
			dependent, err := privateServer.Create(ctx, privatev1.AddOnOperatorsCreateRequest_builder{
				Object: privatev1.AddOnOperator_builder{
					Metadata:  privatev1.Metadata_builder{Name: "concurrent-dependent"}.Build(),
					Title:     "concurrent-dependent",
					Published: new(true),
					Dependencies: []*privatev1.AddOnOperatorLocalReference{
						privatev1.AddOnOperatorLocalReference_builder{Id: first.GetObject().GetId()}.Build(),
					},
				}.Build(),
			}.Build())
			Expect(err).ToNot(HaveOccurred())
			Expect(suiteTx.End(ctx)).To(Succeed())
			suiteTx = nil

			type updateResult struct{ err error }
			results := make(chan updateResult, 2)
			var group sync.WaitGroup
			for _, dependency := range []string{first.GetObject().GetId(), second.GetObject().GetId()} {
				dependency := dependency
				tx, txErr := tm.Begin(context.Background())
				Expect(txErr).ToNot(HaveOccurred())
				group.Add(1)
				go func(tx database.Tx) {
					defer group.Done()
					updateCtx := database.TxIntoContext(context.Background(), tx)
					_, updateErr := privateServer.Update(updateCtx, privatev1.AddOnOperatorsUpdateRequest_builder{
						Object: privatev1.AddOnOperator_builder{
							Id: dependent.GetObject().GetId(),
							Dependencies: []*privatev1.AddOnOperatorLocalReference{
								privatev1.AddOnOperatorLocalReference_builder{Id: dependency}.Build(),
							},
						}.Build(),
						UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"dependencies"}},
					}.Build())
					endErr := tx.End(updateCtx)
					if updateErr == nil {
						updateErr = endErr
					}
					results <- updateResult{err: updateErr}
				}(tx)
			}
			group.Wait()
			close(results)
			for result := range results {
				Expect(result.err).ToNot(HaveOccurred())
			}

			queryTx, err := tm.Begin(context.Background())
			Expect(err).ToNot(HaveOccurred())
			queryCtx := database.TxIntoContext(context.Background(), queryTx)
			var dependencyID string
			err = queryTx.QueryRow(queryCtx, `
				select dependency_id
				from add_on_operator_dependencies
				where operator_id = $1`, dependent.GetObject().GetId()).Scan(&dependencyID)
			Expect(err).ToNot(HaveOccurred())
			Expect(dependencyID).To(BeElementOf(first.GetObject().GetId(), second.GetObject().GetId()))
			Expect(queryTx.End(queryCtx)).To(Succeed())
		})
	})
})
