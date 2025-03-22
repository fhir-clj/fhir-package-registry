-- fhir_packages_canonicals
--$up
drop table if exists fhir_packages.canonical;
--$
create table fhir_packages.canonical (
  id text primary key,

  resourceType text,
  package_name text,
  package_version text,
  _filename text,
  url text,
  version text,
  name text,
  title text,
  status text,
  publisher text,
  description text,
  date text,
  type text,
  kind text,
  experimental text,
  resource jsonb
)
--$
--$down
drop table if exists fhir_packages.canonical;
--$
