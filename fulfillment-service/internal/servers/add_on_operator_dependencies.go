/*
Copyright (c) 2026 Red Hat Inc.

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
	"slices"

	"github.com/osac-project/osac/fulfillment-service/internal/database"
	privatev1 "github.com/osac-project/osac/proto/gen/osac/private/v1"
)

const addOnOperatorDependencyTable = "add_on_operator_dependencies"

const maxAddOnOperatorUpdateDependents = 256

func (s *PrivateAddOnOperatorsServer) validateAndPersistPublishedGraph(
	ctx context.Context, root *privatev1.AddOnOperator,
) (err error) {
	tx, err := database.TxFromContext(ctx)
	if err != nil {
		return err
	}
	defer tx.ReportError(&err)

	// Resolve and validate the updated operator's complete dependency graph
	// against published operators. The selected operators and their order are
	// used below to persist the graph's normalized direct edges.
	resolver := &addOnOperatorReferenceResolver{
		resource: s.publishedOperatorResolver,
		cache:    make(map[addOnOperatorReferenceCacheKey]*privatev1.AddOnOperator),
	}
	graph := newAddOnOperatorGraph(resolver.resolve)
	selected, _, selectedOrder, err := graph.resolveAndExpand(ctx, root.GetMetadata(), []*privatev1.AddOnOperatorReference{
		privatev1.AddOnOperatorReference_builder{Id: root.GetId()}.Build(),
	})
	if err != nil {
		return err
	}

	// Changing this operator can invalidate any published operator that depends
	// on it, directly or transitively. Find those dependents from the existing
	// persisted graph before replacing the root's edges.
	dependentIDs, err := resolveAddOnOperatorDependentClosure(ctx, root.GetId())
	if err != nil {
		return err
	}
	for _, dependentID := range dependentIDs {
		if dependentID == root.GetId() {
			// The root graph was already validated above.
			continue
		}
		// Re-expand each dependent against the updated published graph. This
		// catches cycles or invalid references introduced through the root.
		dependent, resolveErr := resolver.resolve(ctx,
			privatev1.AddOnOperatorReference_builder{Id: dependentID}.Build(),
			root.GetMetadata(), "dependencies")
		if resolveErr != nil {
			return resolveErr
		}
		dependentGraph := newAddOnOperatorGraph(resolver.resolve)
		if _, _, _, resolveErr = dependentGraph.resolveAndExpand(ctx,
			dependent.GetMetadata(), []*privatev1.AddOnOperatorReference{
				privatev1.AddOnOperatorReference_builder{Id: dependentID}.Build(),
			}); resolveErr != nil {
			return resolveErr
		}
	}

	// Persist the validated graph as normalized direct dependency edges.
	for _, operatorID := range selectedOrder {
		operator := selected[operatorID]
		dependencyIDs := make([]string, 0, len(operator.GetDependencies()))
		for _, dependency := range operator.GetDependencies() {
			resolved, resolveErr := resolver.resolve(ctx, dependency, operator.GetMetadata(), "dependencies")
			if resolveErr != nil {
				return resolveErr
			}
			dependencyIDs = append(dependencyIDs, resolved.GetId())
		}
		if err = replaceAddOnOperatorDependencies(ctx, operator.GetId(), dependencyIDs); err != nil {
			return err
		}
	}

	return nil
}

func replaceAddOnOperatorDependencies(ctx context.Context, operatorID string, dependencyIDs []string) (err error) {
	tx, err := database.TxFromContext(ctx)
	if err != nil {
		return err
	}
	defer tx.ReportError(&err)

	ids := slices.Clone(dependencyIDs)
	slices.Sort(ids)
	ids = slices.Compact(ids)

	if _, err = tx.Exec(ctx, `
		delete from `+addOnOperatorDependencyTable+`
		where operator_id = $1`, operatorID); err != nil {
		return fmt.Errorf("delete dependencies for operator %q: %w", operatorID, err)
	}
	if len(ids) == 0 {
		return nil
	}

	if _, err = tx.Exec(ctx, `
		insert into `+addOnOperatorDependencyTable+` (
			operator_id,
			dependency_id
		)
		select $1, dependency_id
		from unnest($2::text[]) as dependency_id`, operatorID, ids); err != nil {
		return fmt.Errorf("insert dependencies for operator %q: %w", operatorID, err)
	}
	return nil
}

// resolveAddOnOperatorDependencyClosure finds the requested operators and all
// operators they directly or transitively depend on.
func resolveAddOnOperatorDependencyClosure(ctx context.Context, rootIDs []string) (ids []string, err error) {
	tx, err := database.TxFromContext(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.ReportError(&err)

	roots := slices.Clone(rootIDs)
	slices.Sort(roots)
	roots = slices.Compact(roots)
	if len(roots) == 0 {
		return nil, nil
	}
	if len(roots) > maxClusterAddOnOperators {
		return nil, fmt.Errorf("resolved add-on operator set exceeds maximum of %d operators", maxClusterAddOnOperators)
	}
	ids = make([]string, 0, len(roots))
	maxDependencyIDs := maxClusterAddOnOperators - len(roots)

	const query = `
		with recursive reachable(dependency_id) as (
			-- Start with the direct dependencies of the requested operators.
			select d.dependency_id
			from ` + addOnOperatorDependencyTable + ` d
			where d.operator_id = any($1::text[])

			union

			-- Follow each discovered dependency, deduplicating converging paths.
			select d.dependency_id
			from reachable g
			join ` + addOnOperatorDependencyTable + ` d
				on d.operator_id = g.dependency_id
		)
		-- Do not sort here: the outer limit lets PostgreSQL stop recursive
		-- evaluation as soon as the graph exceeds the supported node count.
		select dependency_id
		from reachable
		limit $2`

	rows, err := tx.Query(ctx, query, roots, maxDependencyIDs+1)
	if err != nil {
		return nil, fmt.Errorf("expand add-on operator dependencies: %w", err)
	}
	defer rows.Close()

	requestedRootIDs := make(map[string]struct{}, len(roots))
	for _, rootID := range roots {
		requestedRootIDs[rootID] = struct{}{}
	}
	reachableCount := 0
	for rows.Next() {
		var dependencyID string
		if err = rows.Scan(&dependencyID); err != nil {
			return nil, fmt.Errorf("scan add-on operator dependency: %w", err)
		}
		reachableCount++
		if reachableCount > maxDependencyIDs {
			return nil, fmt.Errorf("resolved add-on operator set exceeds maximum of %d operators", maxClusterAddOnOperators)
		}
		if _, isRoot := requestedRootIDs[dependencyID]; !isRoot {
			ids = append(ids, dependencyID)
		}
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate add-on operator dependencies: %w", err)
	}

	slices.Sort(ids)
	return append(roots, ids...), nil
}

// resolveAddOnOperatorDependentClosure finds all operators that directly or
// transitively depend on the specified operator.
func resolveAddOnOperatorDependentClosure(ctx context.Context, dependencyID string) (ids []string, err error) {
	tx, err := database.TxFromContext(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.ReportError(&err)

	const query = `
		with recursive dependents(operator_id) as (
			-- Start with operators that directly depend on the changed operator.
			select d.operator_id
			from ` + addOnOperatorDependencyTable + ` d
			where d.dependency_id = $1

			union

			-- Follow each dependent to operators that depend on it.
			select d.operator_id
			from dependents p
			join ` + addOnOperatorDependencyTable + ` d
				on d.dependency_id = p.operator_id
		)
			select operator_id
			from dependents
			-- Do not sort here: the outer limit lets PostgreSQL stop recursive
			-- evaluation as soon as the update fan-out exceeds its budget.
			limit $2`

	rows, err := tx.Query(ctx, query, dependencyID, maxAddOnOperatorUpdateDependents+1)
	if err != nil {
		return nil, fmt.Errorf("list add-on operator dependents: %w", err)
	}
	defer rows.Close()

	for len(ids) <= maxAddOnOperatorUpdateDependents && rows.Next() {
		var dependentID string
		if err = rows.Scan(&dependentID); err != nil {
			return nil, fmt.Errorf("scan add-on operator dependent: %w", err)
		}
		ids = append(ids, dependentID)
	}
	if len(ids) > maxAddOnOperatorUpdateDependents {
		return nil, fmt.Errorf(
			"published dependency update exceeds the limit of %d affected dependents",
			maxAddOnOperatorUpdateDependents)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate add-on operator dependents: %w", err)
	}
	slices.Sort(ids)
	return ids, nil
}
