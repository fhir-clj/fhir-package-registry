-- fhir_packages_logs
--$up
create table fhir_packages.import (
  id text primary key,
  created_at timestamptz DEFAULT current_timestamp,
  lsn bigint,
  resources_loaded boolean DEFAULT false,
  name text,
  version text
)
--$
--$down
drop table if exists fhir_packages.import;
--$
