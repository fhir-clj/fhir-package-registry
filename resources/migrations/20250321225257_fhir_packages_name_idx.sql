-- fhir_packages_name_idx
--$up
CREATE EXTENSION IF NOT EXISTS pg_trgm;
--$
CREATE INDEX IF NOT EXISTS package_name_idx
ON fhir_packages.package
USING gin (name gin_trgm_ops);
--$
vacuum analyze fhir_packages.package;
--$down
drop index if exists fhir_packages.package_name_idx;
--$
drop EXTENSION IF EXISTS pg_trgm;
