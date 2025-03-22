-- fhir_packages_canonical_idx
--$up
alter table fhir_packages.canonical rename column resourcetype to "resourceType";
--$
update fhir_packages.canonical
set "resourceType" = coalesce("resourceType", resource->>'resourceType'),
 resource = resource - 'resourceType'
--$
CREATE INDEX IF NOT EXISTS canonical_url_idx
ON fhir_packages.canonical
USING gin ((url || '|' || coalesce(version,package_version))  gin_trgm_ops);
--$
CREATE INDEX IF NOT EXISTS canonical_package_idx
ON fhir_packages.canonical (package_name, package_version);
--$
vacuum analyze fhir_packages.canonical;
--$down
alter table fhir_packages.canonical rename column  "resourceType" to resourcetype;
--$
