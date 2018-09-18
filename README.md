Couchbase Elasticsearch Connector
=================================================

[**Download**](https://github.com/couchbaselabs/couchbase-elasticsearch-connector/releases)
| [**Connector Service Documentation**](README-SERVICE.md)
| [**Plugin Documentation**](https://docs.couchbase.com/elasticsearch-connector/3.0/index.html)

The Couchbase Elasticsearch Connector streams documents from Couchbase into Elasticsearch.
It comes in two flavors: a Connector Service, and an Elasticsearch plugin.

The **[Connector Service](README-SERVICE.md)** (available now as a Developer Preview) is designed for
Elasticsearch 5 and newer, including ES 6.
It uses the Couchbase Database Change Protocol (DCP) to receive notifications whenever
a document is modified in Couchbase. The service has an optional distributed mode for horizontal scalability.
It can be configured to use secure connections, and is compatible with X-Pack Security.

The **[Elasticsearch Plugin](README-PLUGIN.md)** is available for Elasticsearch versions 2.2.0 through 5.x. It
uses the Couchbase Cross Datacenter Replication protocol (XDCR) version 1 (also known as CAPI) to make Elasticsearch act like a Couchbase Server node.
The plugin does not support secure connections, and is not compatible with
X-Pack Security.
