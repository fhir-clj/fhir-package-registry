-- fhir_packages
--$up
create schema fhir_packages;
--$
create table fhir_packages.package (
  name text primary key,
  resource jsonb
);
--$
create table fhir_packages.package_version (
  id text primary key,
  name text,
  version text,
  resource jsonb
);
--$
create table fhir_packages.package_dependency (
  id text primary key,
  source_name text,
  source_version text,

  destination_name text,
  destination_version text,
  destination_id text,

  resource jsonb
);
--$
create table fhir_packages.package_file (
  id text primary key,

  package_name text,
  package_version text,

  filename text,
  resource_type text,
  resource_url text,
  resource_version text,
  resource_kind text,
  resource jsonb
);
--$down
drop schema if exists fhir_packages;
--$
