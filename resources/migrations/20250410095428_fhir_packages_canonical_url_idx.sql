-- fhir_packages_canonical_url_idx
--$up
CREATE INDEX IF NOT EXISTS canonical_exact_url_idx
ON fhir_packages.canonical USING btree (url);
--$
vacuum analyze fhir_packages.canonical;
--$down
DROP INDEX IF EXISTS canonical_exact_url_idx;
--$
