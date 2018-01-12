---
---

A Couchbase-Elasticsearch data-replication system consists of three principal components:

- a **Couchbase Server cluster** of one or more nodes
- an **Elasticsearch cluster** of one or more nodes
- the **Elasticsearch Transport Plug-in**, installed in the Elasticsearch environment.

This section provides step-by-step instructions to install the Couchbase Elasticsearch plugin.

## Plugin pre-requisites

- Java (see [installation guide](https://docs.oracle.com/javase/8/))
- Download and install Elasticsearch =< 5.x (see [installation guide](https://www.elastic.co/guide/en/elasticsearch/reference/5.6/install-elasticsearch.html), Elasticsearch 6.0 is not supported by the plugin)
- Download and install Couchbase Server >= 3.0 (see [installation guide](https://www.couchbase.com/downloads))

Verify the Elasticsearch cluster is up and running (the default port is `9200`).

```bash
$ curl localhost:9200

{
  "name" : "K3RqW4F",
  "cluster_name" : "elasticsearch",
  "cluster_uuid" : "Bw-Ta0wDTcekzQIhXZHGkg",
  "version" : {
    "number" : "5.6.5",
    "build_hash" : "6a37571",
    "build_date" : "2017-12-04T07:50:10.466Z",
    "build_snapshot" : false,
    "lucene_version" : "6.6.1"
  },
  "tagline" : "You Know, for Search"
}
```

Verify that Couchbase Server is running.

```bash
$ curl localhost:8092

{"couchdb":"Welcome","version":"v4.5.1-60-g3cf258d","couchbase":"5.0.2-5506-community"}
```

## Installation

Navigate to the Elasticsearch installation directory.

```bash
$ cd /usr/share/elasticsearch
```

Download and install the plug-in package.

```bash
$ bin/elasticsearch-plugin install https://github.com/couchbaselabs/elasticsearch-transport-couchbase/releases/download/3.0.0-cypress/elasticsearch-transport-couchbase-3.0.0-cypress-es5.6.4.zip
```

Replace the plugin URL with the one that matches your Elasticsearch version, all URLs can be found on the [releases page](https://github.com/couchbaselabs/elasticsearch-transport-couchbase/releases).

When the installation is successful, the message "Installed transport-couchbase into /usr/share/elasticsearch/plugins/transport/couchbase" will be logged.

## Configuration

Open the Elasticsearch configuration file (**/etc/elasticsearch/elasticsearch.yml**) and add the following to the end of the file.

```yaml
couchbase.username: <USERNAME>
couchbase.password: <PASSWORD>
couchbase.maxConcurrentRequests: 1024
```

The **username** and **password** credentials will be used again later when configuring Couchbase Server.

Still in the **/usr/share/elasticsearch** directory, configure the Elasticsearch plugin with the following curl command (Couchbase Server must be running).

```bash
$ curl -X PUT http://localhost:9200/_template/couchbase -d @plugins/transport-couchbase/couchbase_template.json
```

When successful, the configuration-routine provides the following response: `{"acknowledged":true}`.

Create an Elasticsearch index to receive the data from the Couchbase bucket. Later, when configuring XDCR, the "remote bucket name" must match the Elasticsearch index name.

```bash
$ curl -X PUT http://localhost:9200/travel-sample

{"acknowledged":true}
```

Now to configure Couchbase Server, open the Couchbase Web Console and select **XDCR > Add Remote Cluster**.

![](img/xdcrInitial.png)

In the dialog, enter the **Cluster Name** of your choice, the **IP/hostname** should correspond to the Elasticsearch cluster and **Username**/**Password** to the credentials previously stored in **elasticsearch.yml**.


![](img/remotecluster.png)