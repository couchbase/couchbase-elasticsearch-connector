---
layout: docs
permalink: connectors/elasticsearch-2.2/performance-tuning.html
---

You can tune the performance of your Couchbase-Elasticsearch data-replication system, by adjusting the number of Couchbase document-fields or documents that you wish to index; by adding nodes to your Elasticsearch cluster; and by changing Couchbase XDCR parameters. This section describes the available options.

## Disable Fields from Indexing

A custom mapping allows fields not required for searching to be disabled, and so omitted from the Elasticsearch indexing process. See [Object Type](https://www.elastic.co/guide/en/elasticsearch/reference/1.4/mapping-object-type.html), in the online Elasticsearch documentation.

## Limit the Number of Index Entries

To limit the number of index entries, you can filter the documents on the Elasticsearch side, by means of the **couchbase.keyFilter** setting: either expressly including or excluding documents whose ID matches a specified regular expression. Included documents are indexed; excluded are omitted. To configure filtering, see the chapter [Advanced Settings](connectors/elasticsearch-2.2/advanced-settings.html).

## Add Elasticsearch Nodes

If your Couchbase Server-cluster experiences a backlog of items in the replication queue, consider adding additional Elasticsearch nodes: this should increase indexing-speed.

## Adjust Concurrent Replication

Couchbase provides XDCR parameters that can be adjusted, to increase replication-speed. See [Tune XDCR Performance](xdcr/xdcr-tuning-performance.html) for detailed information.

Take special note of the parameters `XDCR Source Nozzles per Node` and `XDRC Target Nozzles per Node`, which respectively increase and decrease the maximum concurrent replication performed by a Couchbase node. Their use may be of critical importance if the numbers of concurrent replications, and/or the number of connections per replication prove too great for the Elasticsearch node to handle.

In such cases, errors may be displayed by the Couchbase Web Console. To inspect them, left-click on the XDCR tab, and examine the **Ongoing Replications** section. The following string provides the ID of the CAPI nozzle (a component in XDCR replication), which writes to the target:

```bash
"capi_f4268e62702130298bf87f17cc481219/default/default_172.23.105.146:9091_1 - Connection"
```

This message indicates that data-replication operations are not being completed in the expected timeframes, necessitating replication-retries. To mitigate this problem, reduce the default `XDCR Source Nozzles per Node` setting, and adjust the `XDCR Target Nozzles per Node` setting accordingly.

## Adjust Elasticsearch Node-Configuration

Check your Elasticsearch documentation to ensure that your Elasticsearch nodes are optimally configured for performance. Note the following options:

- When bulk-loading, increase the Elasticsearch `index.refresh_interval` setting, in order to improve indexing-performance. This setting determines how quickly Elasticsearch makes the indexed documents available to query. For a large bulk-load, where real-time indexing is not needed, a high setting, such as 30 seconds, may significantly increase throughput.
- In the event that specific documents are known typically to be queried together, document routing within the plug-in can be configured such that the documents always reside on the same Elasticsearch shard; in order to boost query performance.
- Mapping documents to specific Elasticsearch types, and then searching within a specific type, is faster than searching the entire Elasticsearch index.