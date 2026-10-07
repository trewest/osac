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

-- AddOnOperator references in active ClusterCatalogItems are strong dependencies.
-- The policy can store references in either its locked or editable default branch.
create or replace function check_add_on_operator_not_in_use() returns trigger as $$
begin
  if (
    (old.deletion_timestamp = 'epoch' and new.deletion_timestamp <> 'epoch')
    or (
      coalesce(old.data->>'published', 'false') = 'true'
      and coalesce(new.data->>'published', 'false') <> 'true'
    )
  ) and exists (
    select 1
    from cluster_catalog_items c
    cross join lateral jsonb_array_elements(
      coalesce(c.data->'fields'->'add_on_operators'->'locked'->'items', '[]'::jsonb)
      || coalesce(c.data->'fields'->'add_on_operators'->'editable'->'default_value'->'items', '[]'::jsonb)
    ) reference
    where c.deletion_timestamp = 'epoch'
      and (
        reference->>'id' = old.id
        or (coalesce(reference->>'id', '') = '' and reference->>'name' = old.name)
      )
  ) then
    raise exception using
      errcode = 'Z0003',
      message = format('cannot modify add-on operator ''%s'': it is in use by at least one cluster catalog item', old.name);
  end if;

  return new;
end;
$$ language plpgsql;

create trigger check_add_on_operator_not_in_use
  before update on add_on_operators
  for each row
  execute function check_add_on_operator_not_in_use();
