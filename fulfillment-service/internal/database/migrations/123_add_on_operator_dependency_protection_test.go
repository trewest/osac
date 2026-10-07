/*
Copyright (c) 2026 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
*/

package migrations

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2/dsl/core"
	. "github.com/onsi/gomega"
)

var _ = DescribeMigration("Add-on operator dependency protection", func() {
	BeforeEach(func(ctx context.Context) {
		Expect(tool.Migrate(ctx, 123)).To(Succeed())
	})

	It("protects a dependency of an active catalog item from deletion", func(ctx context.Context) {
		_, err := conn.Exec(ctx, `
			insert into add_on_operators (id, name, tenant, data)
			values
				('operator-dependency', 'operator-dependency', 'shared', '{"published":true}'::jsonb),
				('operator-root', 'operator-root', 'shared', '{"published":true,"dependencies":[{"id":"operator-dependency","name":"operator-dependency"}]}'::jsonb)`)
		Expect(err).ToNot(HaveOccurred())
		_, err = conn.Exec(ctx, `
			insert into cluster_catalog_items (id, name, tenant, data)
			values ('catalog-dependency', 'catalog-dependency', 'system',
			'{"fields":{"add_on_operators":{"locked":{"items":[{"id":"operator-root"}]}}}}'::jsonb)`)
		Expect(err).ToNot(HaveOccurred())

		_, err = conn.Exec(ctx, `update add_on_operators set deletion_timestamp = now() where id = 'operator-dependency'`)
		var pgError *pgconn.PgError
		Expect(errors.As(err, &pgError)).To(BeTrue())
		Expect(pgError.Code).To(Equal("Z0003"))
	})

	It("protects a dependency of an active catalog item from unpublishing", func(ctx context.Context) {
		_, err := conn.Exec(ctx, `
			insert into add_on_operators (id, name, tenant, data)
			values
				('operator-dependency', 'operator-dependency', 'shared', '{"published":true}'::jsonb),
				('operator-root', 'operator-root', 'shared', '{"published":true,"dependencies":[{"id":"operator-dependency","name":"operator-dependency"}]}'::jsonb)`)
		Expect(err).ToNot(HaveOccurred())
		_, err = conn.Exec(ctx, `
			insert into cluster_catalog_items (id, name, tenant, data)
			values ('catalog-dependency', 'catalog-dependency', 'system',
			'{"fields":{"add_on_operators":{"editable":{"default_value":{"items":[{"id":"operator-root"}]}}}}}'::jsonb)`)
		Expect(err).ToNot(HaveOccurred())

		_, err = conn.Exec(ctx, `update add_on_operators set data = jsonb_set(data, '{published}', 'false'::jsonb) where id = 'operator-dependency'`)
		var pgError *pgconn.PgError
		Expect(errors.As(err, &pgError)).To(BeTrue())
		Expect(pgError.Code).To(Equal("Z0003"))
	})

	It("protects dependencies of published operators without catalog items", func(ctx context.Context) {
		_, err := conn.Exec(ctx, `
			insert into add_on_operators (id, name, tenant, data)
			values
				('operator-dependency', 'operator-dependency', 'shared', '{"published":true}'::jsonb),
				('operator-root', 'operator-root', 'shared', '{"published":true,"dependencies":[{"id":"operator-dependency","name":"operator-dependency"}]}'::jsonb)`)
		Expect(err).ToNot(HaveOccurred())

		_, err = conn.Exec(ctx, `update add_on_operators set deletion_timestamp = now() where id = 'operator-dependency'`)
		var pgError *pgconn.PgError
		Expect(errors.As(err, &pgError)).To(BeTrue())
		Expect(pgError.Code).To(Equal("Z0003"))
	})

	It("terminates cyclic dependency traversal", func(ctx context.Context) {
		_, err := conn.Exec(ctx, `
			insert into add_on_operators (id, name, tenant, data)
			values
				('operator-a', 'operator-a', 'shared', '{"published":true,"dependencies":[{"id":"operator-b","name":"operator-b"}]}'::jsonb),
				('operator-b', 'operator-b', 'shared', '{"published":true,"dependencies":[{"id":"operator-a","name":"operator-a"}]}'::jsonb)`)
		Expect(err).ToNot(HaveOccurred())
		_, err = conn.Exec(ctx, `
			insert into cluster_catalog_items (id, name, tenant, data)
			values ('catalog-cycle', 'catalog-cycle', 'system',
			'{"fields":{"add_on_operators":{"locked":{"items":[{"id":"operator-a"}]}}}}'::jsonb)`)
		Expect(err).ToNot(HaveOccurred())

		_, err = conn.Exec(ctx, `update add_on_operators set deletion_timestamp = now() where id = 'operator-b'`)
		var pgError *pgconn.PgError
		Expect(errors.As(err, &pgError)).To(BeTrue())
		Expect(pgError.Code).To(Equal("Z0003"))
	})
})
