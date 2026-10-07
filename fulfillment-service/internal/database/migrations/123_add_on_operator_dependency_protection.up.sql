--
-- Copyright (c) 2026 Red Hat, Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
-- the License. You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
-- language governing permissions and limitations under the License.
--

-- Protect dependencies of active catalog roots and published shared operators.
-- The graph traversal is cycle-safe and runs while the target operator row is locked by the trigger update.
create or replace function check_add_on_operator_not_in_use() returns trigger as $$
begin
  if (
    (old.deletion_timestamp = 'epoch' and new.deletion_timestamp <> 'epoch')
    or (
      coalesce(old.data->>'published', 'false') = 'true'
      and coalesce(new.data->>'published', 'false') <> 'true'
    )
  ) and exists (
    with recursive catalog_roots as (
      select distinct operator.id
      from cluster_catalog_items catalog_item
      cross join lateral jsonb_array_elements(
        coalesce(catalog_item.data->'fields'->'add_on_operators'->'locked'->'items', '[]'::jsonb)
        || coalesce(catalog_item.data->'fields'->'add_on_operators'->'editable'->'default_value'->'items', '[]'::jsonb)
      ) reference
      join add_on_operators operator
        on operator.deletion_timestamp = 'epoch'
        and operator.tenant = 'shared'
        and coalesce(operator.project, '') = ''
        and (
          reference->>'id' = operator.id
          or (coalesce(reference->>'id', '') = '' and reference->>'name' = operator.name)
        )
      where catalog_item.deletion_timestamp = 'epoch'
    ),
    published_roots as (
      select operator.id
      from add_on_operators operator
      where operator.deletion_timestamp = 'epoch'
        and operator.id <> old.id
        and operator.tenant = 'shared'
        and coalesce(operator.project, '') = ''
        and coalesce(operator.data->>'published', 'false') = 'true'
    ),
    roots as (
      select id from catalog_roots
      union
      select id from published_roots
    ),
    operator_graph(root_id, operator_id, path) as (
      select roots.id, roots.id, array[roots.id]::text[]
      from roots
      union all
      select graph.root_id, dependency.id, graph.path || dependency.id
      from operator_graph graph
      join add_on_operators parent on parent.id = graph.operator_id
      cross join lateral jsonb_array_elements(coalesce(parent.data->'dependencies', '[]'::jsonb)) reference
      join add_on_operators dependency
        on dependency.deletion_timestamp = 'epoch'
        and dependency.tenant = 'shared'
        and coalesce(dependency.project, '') = ''
        and (
          reference->>'id' = dependency.id
          or (coalesce(reference->>'id', '') = '' and reference->>'name' = dependency.name)
        )
      where not dependency.id = any(graph.path)
    )
    select 1
    from operator_graph
    where operator_id = old.id
    limit 1
  ) then
    raise exception using
      errcode = 'Z0003',
      message = format('cannot modify add-on operator ''%s'': it is in use by an active catalog item or published operator dependency', old.name);
  end if;

  return new;
end;
$$ language plpgsql;
