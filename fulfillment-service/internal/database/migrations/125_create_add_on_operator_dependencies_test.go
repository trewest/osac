/*
Copyright (c) 2026 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
*/

package migrations

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2/dsl/core"
	. "github.com/onsi/gomega"
)

var _ = DescribeMigration("Create add-on operator dependency table", func() {
	BeforeEach(func(ctx context.Context) {
		Expect(tool.Migrate(ctx, 125)).To(Succeed())
	})

	It("stores canonical direct edges and rejects duplicates", func(ctx context.Context) {
		_, err := conn.Exec(ctx, `
			insert into add_on_operators (id, name, tenant, data)
			values
				('ao-dependent', 'dependent', 'shared', '{}'),
				('ao-dependency', 'dependency', 'shared', '{}')`)
		Expect(err).ToNot(HaveOccurred())

		_, err = conn.Exec(ctx, `
			insert into add_on_operator_dependencies (operator_id, dependency_id)
			values ($1, $2)`, "ao-dependent", "ao-dependency")
		Expect(err).ToNot(HaveOccurred())

		var count int
		err = conn.QueryRow(ctx, `
			select count(*)
			from add_on_operator_dependencies
			where operator_id = $1 and dependency_id = $2`,
			"ao-dependent", "ao-dependency").Scan(&count)
		Expect(err).ToNot(HaveOccurred())
		Expect(count).To(Equal(1))

		_, err = conn.Exec(ctx, `
			insert into add_on_operator_dependencies (operator_id, dependency_id)
			values ($1, $2)`, "ao-dependent", "ao-dependency")
		Expect(err).To(HaveOccurred())
		var pgErr *pgconn.PgError
		Expect(errors.As(err, &pgErr)).To(BeTrue())
		Expect(pgErr.Code).To(Equal("23505"))
	})

	It("requires both operators to exist", func(ctx context.Context) {
		_, err := conn.Exec(ctx, `
			insert into add_on_operators (id, name, tenant, data)
			values ('ao-dependent', 'dependent', 'shared', '{}')`)
		Expect(err).ToNot(HaveOccurred())

		_, err = conn.Exec(ctx, `
			insert into add_on_operator_dependencies (operator_id, dependency_id)
			values ($1, $2)`, "ao-dependent", "missing")
		Expect(err).To(HaveOccurred())
		var pgErr *pgconn.PgError
		Expect(errors.As(err, &pgErr)).To(BeTrue())
		Expect(pgErr.Code).To(Equal("23503"))
	})
})
