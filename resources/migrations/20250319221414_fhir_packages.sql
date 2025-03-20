-- fhir_packages
--$up
create schema fhir_packages;
--$
create table fhir_packages.package (
  id text primary key,
  name text,
  type text,
  title text,
  description text,
  "fhirVersions" text[],
  version text,
  author text,
  canonical text,
  homepage text,
  license text,

  resource jsonb
);
--$
create table fhir_packages.package_dependency (
  id text primary key,
  source_name text,
  source_version text,

  destination_name text,
  destination_version text,
  resource jsonb
);
--$
create table fhir_packages.canonical (
  id text primary key,

  package_name text,
  package_version text,

  filename text,
  type text,
  url text,
  version text,
  kind text,
  resource jsonb
);
--$down
drop schema if exists fhir_packages cascade;
--$
