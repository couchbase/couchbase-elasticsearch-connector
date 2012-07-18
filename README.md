Couchbase XDCR Transport Plugin for ElasticSearch
=================================================

This plugin makes your ElasticSearch node appear like a Couchbase Server node.  After installation you can use the Cross-datacenter Replication (XDCR) feature of Couchbase Server 2.0 to transfer data continuously.

Installation
============

In order to install the plugin, simply run: `bin/plugin -install couchbaselabs/elasticsearch-transport-couchbase/1.0.0`.

    +---------------------------------------------------+
    |  Plugin        |  Couchbase    | ElasticSearch    |
    +---------------------------------------------------+
    | master         |  2.0 (1453)   | 0.19.2           |
    +---------------------------------------------------+
    | 1.0.0-SNAPSHOT |  2.0(1453)    | 0.19.2           |
    +---------------------------------------------------+
    
Configuration
=============

- couchbase.port - the port the plugin will listen on, default 8091

Usage
=====

Preparing ElasticSearch
1. Install the plugin on each node in your cluster.
2. Create an ElasticSearch index to store the data from Couchbase (ie. default)
3. Create another index with the same name followed by "_master" (ie. default_master)  This is used to store replication checkpoints.

Preparing Couchbase

1. Navigate to the Couchbase Server admin interface.
2. Select the Replications tab.
3. Press the button labeled "Create Cluster Reference"
4. Choose a name for your ElasticSearch cluster
5. In the IP/Hostname field provide an address of one of the nodes in your ES cluster
6. Currently Username/Password are not used
7. Press the "Save" button

Starting Data Transfer

1. Press the button labeled "Create Replication"
2. Select the bucket from source cluster you wish to send to ElasticSearch
3. Next select the cluster you defined in step 4.
4. Type in the name of the ElasticSearch index you wish to store the data in.  This index must already exist.

Building
========

This module is built using maven.

    mvn package