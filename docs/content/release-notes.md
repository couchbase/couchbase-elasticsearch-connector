---
layout: docs
permalink: connectors/elasticsearch-2.2/release-notes.html
---

This section provides a set of Release Notes for successive versions of the Elasticsearch Transport Plug-in, provided by Couchbase. Each set of notes provide details of changes and additions that have been made.

## Version Compatibility

The [GitHub releases](https://github.com/couchbaselabs/elasticsearch-transport-couchbase/releases) page lists all the plugin variants. In a given release, the plugin's filename is of the form **elasticsearch-transport-couchbase-${PLUGIN\_VERSION}-alder-es${ES\_VERSION}.zip**. It is imperative to use the file where **ES_VERSION** matches your Elasticsearch cluster version. For example, to use the 3.0.0 version of the plugin with Elasticsearch 5.6.4, you will need to download **elasticsearch-transport-couchbase-3.0.0-cypress-es5.6.4.zip**.

## Elasticsearch Plug-in 3.0

With [this release](https://github.com/couchbaselabs/elasticsearch-transport-couchbase/releases/tag/3.0.0-cypress), the Couchbase plugin for Elasticsearch moves to a new versioning and branch management strategy that allows for simultaneous releases across ES versions (see [Version Compatibility](release-notes.html#version-compatibility)). All the versions are expected to work and are supported. However, versions which have received more testing and are **officially** supported are:

- 3.0.0-cypress-es5.6.4
- 3.0.0-birch-es5.2.2
- 3.0.0-alder-es2.4.0

Don't be alarmed by the major version bump; upgrading from version 2.x of the plugin should be seamless, and is recommended for all users. The code has just been relabeled for easier maintenance.

### New Feature

- Adds the `couchbase.pipeline` config for specifying the ingestion pipeline.

### Enhancements

- Improves logging for indexing errors.
- Uses Dropwizard Metrics to collect and log richer stats.

### Bug Fixes

- [CBES-48](https://issues.couchbase.com/browse/CBES-48): Connection counter leak could cause spurious `TooManyConcurrentConnections` errors.
- [#153](https://github.com/couchbaselabs/elasticsearch-transport-couchbase/issues/153): ClassCastException error when the TTL is of type `Long` instead of `Integer`.

### Known issues

The Elasticsearch Plug-in does not support IPv6. So to use the plugin, the Couchbase Server and Elasticsearch clusters will need to run on instances which are addressable with IPv4.

## Elasticsearch Plug-in 2.2

This release note applies to the 2.2 version of the Elasticsearch Transport Plug-in (February 2017). It adds a number of bug fixes. See [Installation and Configuration](connectors/elasticsearch-2.2/install-and-config.html) for versioning and compatibility information.

## Elasticsearch Plug-in 2.1.1

This release note applies to the 2.1.1 version of the Elasticsearch Transport Plug-in (September 2015). It adds compatibility with newer Elasticsearch versions up to 1.7.x, multiple new features, and quite a few bug fixes, including several that solve issues found in 2.1.0. In particular, this release fixes a long-standing bug with an incorrect concurrent bulk request counter, which could eventually cause the plug-in to stop accepting requests from Couchbase Server altogether.

Some configuration option-names changed.

|Elasticsearch plug-in version|Couchbase versions|Elasticsearch versions|
|:----------------------------|:-----------------|:---------------------|
|2.1.1|2.5.x - 4.x|1.3.0 - 1.7.x|

## Elasticsearch Plug-in 2.0

This release note is for the Elasticsearch plug-in release 2.0 GA (October 2014). Elasticsearch plug-in version 2.0 is compatible with:

- Elasticsearch 1.3.0.
- Couchbase Server 3.0
- Couchbase Server 2.5.x (backward compatible)

The new feature(s) available in Elasticsearch Plugin v2.0:

- Support more than one document type in Elasticsearch. ([MB-12284](https://www.couchbase.com/issues/browse/MB-12284))

The following are known issues:

- The `att_reason` value for non-JSON documents changed from non-JSON mode to invalid_json. If a Couchbase cluster has a lot of deletes, the Elasticsearch log could fill up with a lot of messages. ([CBES-31](http://www.couchbase.com/issues/browse/CBES-31))

## Elasticsearch Plug-in 1.3.0

This release note is for the Elasticsearch plug-in release 1.3.0 GA (April 2014). This release is compatible only with Elasticsearch 1.0.1.

This release is compatible with Couchbase Server 2.5.x, and it is backward compatible with earlier 2.x releases.

- Support for new XDCR checkpoint protocol. ([CBES-26](https://www.couchbase.com/issues/browse/CBES-26))
- Fixed failure handling due to bounded queue with Elasticsearch 1.x. ([CBES-27](https://www.couchbase.com/issues/browse/CBES-27))

## Elasticsearch Plug-in 1.2.0

This release note is for the Elasticsearch plug-in release 1.2.0 GA (October 2013). This release adds compatibility with Elasticsearch 0.90.5.

This release is compatible with Couchbase Server 2.2, and it is backward compatible with earlier 2.x releases.

## Elasticsearch Plug-in 1.1.0

This release note is for the Elasticsearch plug-in release 1.1.0 GA (August 2013). This release adds compatibility with Elasticsearch 0.90.2.

## Elasticsearch Plug-in 1.0.0

This release note is for the Elasticsearch plug-in release 1.0.0 GA (February 2013). This is the first general availability (GA) release. It contains the following enhancements and bug fixes:

- Now compatible with version 0.20.2 of Elasticsearch.
- Now supports document expiration using Elasticsearch TTL.
- Now supports XDCR conflict resolution to reduce bandwidth usage in some cases.
- Fixed Couchbase index template to allow searching on the document metadata.
- Fixed data corruption under high load. ([CBES-11](http://www.couchbase.com/issues/browse/CBES-11))
- Fixed recognition of non-JSON documents. ([CBES-11](http://www.couchbase.com/issues/browse/CBES-11))
- Improved log information when indexing stub documents.

## Elasticsearch Plug-in 1.0.0 Beta

This is the beta release of the Couchbase plug-in for Elasticsearch 1.0.0 Beta (February 2013).
