# FHIR Package registry implementation

This is a FHIR Package Registry implementation, which is extremely scalable and fast.
And by serving most of the API from GCP bucket, it is able to handle a large number of requests.

It is heavily based on GCP bucket, delegating most of the serving API 
to the bucket for performance reasons.

Bucket has the following layout:

* `-/` - contains package archives in format `<package_name>-<version>.tgz`,
  this is a "source of truth" for packages.
* `pkgs/<package_name>` - contains project files in JSON format,
  this is a "view" on top of the packages.
* `rs/<package_name>-<version>.ndjson.gz` - contains canonical resources from packages 
   in NDJSON format.
* `pkgs.ndjson.gz` - contains all packages in NDJSON format.
* `feed.ndjson.gz` - contains the last 1000 loaded packages, each package has an `lsn` field
   which is incremented on each package load. If you want to consume a change stream,
   remember the last lsn from `feed.ndjson.gz` and read from `pkgs.ndjson.gz` starting from that lsn.
 
## Architecture

There are two main components:

* storage - which manage packages in GCP bucket and all the metadata
* server -  which serve the search and UI from metadata indexed in PostgreSQL database

### Storage

Storage has a several jobs:

* sync packages from FHIR packages2 registry into `-/` directory
* indexing job, which update `pkgs/`, `rs/` directories and `pkgs.ndjson.gz` with `feed.ndjson.gz`
* job which consumes `feed.ndjson.gz` and update PostgreSQL database

### Server

Provides lookup API for packages and UI pages for each package and its versions.
As well canonicals lookup and some analytics pages.

## Usage

Get all packages:

```
curl -s https://fs.get-ig.org/pkgs.ndjson.gz | gunzip | jq
```

Get package json for all versions:
```
curl -s https://fs.get-ig.org/pkgs/<package_name> |  jq
```

Working with npm:

```
npm info    --registry https://fs.get-ig.org/pkgs hl7.fhir.us.core
npm install --registry https://fs.get-ig.org/pkgs hl7.fhir.us.core
npm install --registry https://fs.get-ig.org/pkgs hl7.fhir.us.core@8.0.0-ballot

# or
export NPM_CONFIG_REGISTRY=https://fs.get-ig.org/pkgs

npm info    hl7.fhir.us.core
npm install hl7.fhir.us.core
npm install hl7.fhir.us.core@8.0.0-ballot
```

Get canonical resources for a package version:
```
curl -s https://fs.get-ig.org/rs/<package_name>-<version>.ndjson.gz | gunzip | jq
```

Get feed - last 1000 loaded packages:

```
curl -s https://fs.get-ig.org/feed.ndjson.gz | gunzip | jq
```

## TODO

* handle search (probably using load balancer) - test with `npm search`
* support upload new package
* support upload from simplifier
