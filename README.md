Couchbase Elasticsearch Connector
=================================================

[**Download**](https://github.com/couchbaselabs/couchbase-elasticsearch-connector/releases)
| [**Instruction Manual**](https://developer.couchbase.com/documentation/server/current/connectors/elasticsearch/overview.html)

The Couchbase Elasticsearch Connector streams documents from Couchbase into Elasticsearch.
It comes in two flavors: a Connector Service, and an Elasticsearch plugin.

The **[Connector Service](README-JAVA-APP.md)** (arriving August 31st, 2018 as a Developer Preview) is designed for
Elasticsearch 5 and newer, including ES 6.
It uses the Couchbase Database Change Protocol (DCP) to receive notifications whenever
a document is modified in Couchbase. The service has an optional distributed mode for horizontal scalability.
It can be configured to use secure connections, and is compatible with X-Pack Security.

The **[Elasticsearch plugin](README-PLUGIN.md)** is available for Elasticsearch versions 2.2.0 through 5.x. It
uses the Couchbase Cross Datacenter Replication protocol (XDCR) version 1 (also known as CAPI) to make Elasticsearch act like a Couchbase Server node.
The plugin does not support secure connections, and is not compatible with
X-Pack Security.
