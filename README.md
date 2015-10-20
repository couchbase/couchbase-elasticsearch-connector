Couchbase Transport Plugin for ElasticSearch
=================================================

For a pre-built binary package and instruction manual, see the [Couchbase Downloads Page](http://www.couchbase.com/nosql-databases/downloads) and the [Couchbase Connector Guide for Elasticsearch](http://developer.couchbase.com/documentation/server/4.0/connectors/elasticsearch-2.1/elastic-intro.html)

This plugin makes your ElasticSearch node appear like a Couchbase Server node.  After installation you can use the Cross-datacenter Replication (XDCR) feature of Couchbase Server 2.5 / 3.x to transfer data continuously.

Installation
============

To install the plugin, run the following command from your ElasticSearch installation folder:

    bin/plugin -install transport-couchbase -url http://packages.couchbase.com.s3.amazonaws.com/releases/elastic-search-adapter/2.1.1/elasticsearch-transport-couchbase-2.1.1.zip
    
Version Compatibility:

    +------------------------------------------------------------------+
    |  Plugin                       |  Couchbase    | ElasticSearch    |
    +------------------------------------------------------------------+
    | master                        |  3.x, 2.5.x   | 1.3.0 - 1.7.x    |
    +------------------------------------------------------------------+
    | 2.1                           |  3.x, 2.5.x   | 1.3.0 - 1.7.x    |
    +------------------------------------------------------------------+
    | 2.0                           |  3.x, 2.5.x   | 1.3.0            |
    +------------------------------------------------------------------+
    | 1.3.0                         |  2.5.x        | 1.1.0            |
    +------------------------------------------------------------------+
    | 1.2.0                         |  2.2          | 0.90.5           |
    +------------------------------------------------------------------+
    | 1.1.0                         |  2.0          | 0.90.2           |
    +------------------------------------------------------------------+
    | 1.0.0-dp                      |  2.0 (beta)   | 0.19.9           |
    +------------------------------------------------------------------+
    | 1.0.0-beta                    |  2.0.0        | 0.20.2           |
    +------------------------------------------------------------------+
    | 1.0.0                         |  2.0.0        | 0.20.2           |
    +------------------------------------------------------------------+
    
# Configuration #

Configuration for the plugin is specified as part of the ElasticSearch config file (usually elasticsearch.yml) and is currently only read when ElasticSearch starts. Dynamic configuration support is planned for the future.

## Basic Settings ##

- **couchbase.port** - The port the plugin will listen on, default `9091`
- **couchbase.username** - The username for HTTP basic auth, default `Administrator`
- **couchbase.password** - The password for HTTP basic auth, no default
- **couchbase.num_vbuckets** - The number of vbuckets that ElasticSearch should pretend to have (default on Mac is 64, 1024 on all other platforms)  This value MUST match the number of vbuckets on the source Couchbase cluster.
- **couchbase.maxConcurrentRequests** - The number of concurrent requests that the plugin will allow, default 1024 (lower this if the load on the machine gets too high)

## Advanced Settings ##

- **couchbase.ignoreFailures** - Enabling this flag will cause the plugin to return a success status to Couchbase even if it cannot index some of the documents. This will prevent the XDCR replication from being stalled due to indexing errors in ElasticSearch, for example when a schema change breaks some of the ES type mappings. Default is `false`.
- **couchbase.ignoreDeletes** - Specifying one or more index names here will cause the plugin to ignore document deletion and expiration for those indexes. This can be used to turn ElasticSearch into a sort of searchable archive for a Couchbase bucket. Note that this also means that the index will continue to grow indefinitely.
- **couchbase.wrapCounters** - Enabling this flag will cause the plugin to wrap integer values from Couchbase, which are not valid JSON documents, in a simple document before indexing them in ElasticSearch. The resulting document is in the format `{ "value" : <value> }` and is stored under the ID of the original value from Couchbase.

### Mapping Couchbase documents to ElasticSearch types ###

- **couchbase.typeSelector** - The type selector class to use for mapping documents to types.
	- **`org.elasticsearch.transport.couchbase.capi.DefaultTypeSelector`** - Maps all documents to the specified type. As the name implies, this is the default type selector and can be omitted from the configuration file.
		- **couchbase.typeSelector.defaultDocumentType** - The document type to which the DefaultTypeSelector will map all documents. Defaults to "couchbaseDocument".
		- **couchbase.typeSelector.checkpointDocumentType** - The document type to which replication checkpoint documents will be mapped. Defaults to "couchbaseCheckpoint".
	- **`org.elasticsearch.transport.couchbase.capi.DelimiterTypeSelector`** - If the document ID is of the format `<type><delimiter><*>`, this type selector will map these documents to the type `<type>`, otherwise it will use the `DefaultTypeSelector` for the type mapping. The default delimiter is `:`, so for example a document with the ID `user:123` will be indexed under the type `user`.
		- **couchbase.typeSelector.documentTypeDelimiter** - Optional. The delimiter to use for the `DelimiterTypeSelector`. Default is `:`. 
	- **`org.elasticsearch.transport.couchbase.capi.GroupRegexTypeSelector`** - Maps documents that match the specified regular expression with a capture group named `type`. If the document doesn't match the regular expression, or the regular expression doesn't define a capture group named `type`, the `DefaultTypeSelector` is used instead.
		- **couchbase.typeSelector.documentTypesRegex** - Specified the regular expression for mapping Couchbase document IDs to ElasticSearch types. Example: `^(?<type>\w+)::.+$` will map document IDs of the format `<type>::<stuff>` to the type `<type>`, so the ID `user::123` will be indexed under the type `user`.
	- **`org.elasticsearch.transport.couchbase.capi.RegexTypeSelector`** - Maps document IDs that match the specified regular expressions to the named types. If the ID doesn't match any of the specified expressions, `DefaultTypeSeletor` is used to select the type.
		- **couchbase.typeSelector.documentTypesRegex.*** - Specifies a regular expression with a named type. For example, `couchbase.typeSelector.documentTypesRegex.users: ^user-.+$` will map all document IDs that start with the string `user-` to the type `users`.

### Mapping parent-child relationships ###

- **couchbase.parentSelector** - The parent selector class to use for mapping child documents to parents. Note that because of the nature of XDCR, it's possible that the child document will be replicated before the parent, leading to unpredictable behaviour on the ElasticSearch side.
	- **`org.elasticsearch.transport.couchbase.capi.DefaultParentSelector`** - Maps documents to parents according to a predefined map of types to field names. 
		- **couchbase.parentSelector.documentTypeParentFields.*** - Specifies which document field contains the ID of the parent document for that particular type. For example, `couchbase.parentSelector.documentTypeParentFields.order: doc.user_id` will set the parent ID of all documents in the type `order` to the value of the user_id field.
	- **`org.elasticsearch.transport.couchbase.capi.RegexParentSelector`** - Maps documents to parents according to a specified regular expression with the capture group `parent`. Optionally lets you specify the format for the parent document ID.
		- **couchbase.parentSelector.documentTypesParentRegex.*** - A named regular expression for matching the parent document ID. For example, `couchbase.documentTypesParentRegex.typeA: ^typeA::(?<parent>.+)` with the document ID `typeA::123` will use `123` as the parent document ID.
		- **couchbase.parentSelector.documentTypesParentFormat.*** - Specifies an optional format for the parent document ID matched by the regular expression above. Uses `<parent>` as the placeholder for the matched ID. For example, `couchbase.documentTypesParentFormat.typeA: parentType::<parent>` with the previous example will produce the parent document ID `parentType::123`.

### Specifying custom document routing ###

- **couchbase.documentTypeRoutingFields.*** - A mapping of types to custom document routing paths. For example, specifying `couchbase.documentTypeRoutingFields.users: user_id` will use the field `user_id` as the custom routing path for type `users`.

### Filtering documents on the ElasticSearch side ###
- **couchbase.keyFilter** - The document filter class to use for filtering documents on the plugin side. Note that Couchbase sends all documents through XDCR no matter what, the document filter simply chooses whether to index or ignore certain documents according to their ID.
	- **`org.elasticsearch.transport.couchbase.capi.DefaultKeyFilter`** - The default filter, which lets all documents through. Can be omitted from the configuration file.
	- **`org.elasticsearch.transport.couchbase.capi.RegexKeyFilter`** - The
		- **couchbase.keyFilter.type** - `[include|exclude]` Specifies whether the filter will include or exclude the documents based on the matched regular expression. If `include`, then only documents with IDs that match one of the regular expressions will be indexed. If `exclude`, then only documents that do **not** match **any** of the regular expressions will be indexed.   
		- **couchbase.keyFilter.keyFiltersRegex.*** - Specifies one or more regular expressions to match against the document ID before indexing them in ElasticSearch. For example, `couchbase.keyFilter.type: exclude` + `couchbase.keyFilter.keyFiltersRegex.temp: ^temp.*$` will cause the plugin to ignore any documents whose IDs start with `temp`.


Couchbase Document Expiration
=============================

If you use the document expiration feature of Couchbase Server to expire documents after a specified TTL, you must enable the corresponding feature in your ElasticSearch mapping.  There is some cost associated with enabling this feature, so it is left disabled by default.

See this page in the ElasticSearch guide for more information about enabling this feature:

http://www.elasticsearch.org/guide/reference/mapping/ttl-field.html


Usage
=====

Preparing ElasticSearch

1. Install the plugin on each node in your cluster.
2. Install the Couchbase template
    curl -XPUT http://localhost:9200/_template/couchbase -d @plugins/transport-couchbase/couchbase_template.json
3. Create an ElasticSearch index to store the data from Couchbase (ie. default)

Preparing Couchbase

1. Navigate to the Couchbase Server admin interface.
2. Select the Replications tab.
3. Press the button labeled "Create Cluster Reference"
4. Choose a name for your ElasticSearch cluster
5. In the IP/Hostname and field provide an address and port of one of the nodes in your ES cluster (127.0.0.1:9091)
6. Enter the Username and Password corresponding to your "couchbase.username" and "couchbase.password" settings in ElasticSearch
7. Press the "Save" button

Starting Data Transfer

1. Press the button labeled "Create Replication"
2. Select the bucket from source cluster you wish to send to ElasticSearch
3. Next select the cluster you defined in step 4.
4. Type in the name of the ElasticSearch index you wish to store the data in.  This index must already exist.
5. If you are using Couchbase Server 2.2 or later, click Advanced settings and change the XDCR Protocol setting to Version 1

Building
========

This module is built using maven.  It depends on another project which is not in any public maven repositories, see https://github.com/couchbaselabs/couchbase-capi-server and run `mvn install` on that first.

Then in this project run

    mvn package
    
The final plugin package will be in the target/releases folder.
