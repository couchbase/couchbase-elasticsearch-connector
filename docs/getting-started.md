---
---

A Couchbase-Elasticsearch data-replication system consists of three principal components:

- a **Couchbase Server-cluster** of one or more nodes
- an **Elasticsearch cluster** of one or more nodes
- the **Elasticsearch Transport Plug-in**, installed in the Elasticsearch environment.

This section provides a step-by-step procedure, whereby the three components can be installed, configured, and run.

## Pre-requisites

- Ubuntu 14.04 or 16.04
- Java

## Installation

1. Download the Elasticsearch 2.4.0 package for Ubuntu.

	```bash
	$ wget https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-2.4.6.deb
	```

2. Install the package.

	```bash
	$ dpkg -i elasticsearch-2.4.6.deb
	```

3. Navigate to the Elasticsearch installation directory.

	```bash
	$ cd /usr/share/elasticsearch
	```

4. Download and install the plug-in package.

	```bash
	$ bin/plugin install https://github.com/couchbaselabs/elasticsearch-transport-couchbase/releases/download/3.0.0-cypress/elasticsearch-transport-couchbase-3.0.0-alder-es2.4.6.zip
	```

When the installation is successful, the message "Installed transport-couchbase into /usr/share/elasticsearch/plugins/transport/couchbase" is logged.

5. Open the Elasticsearch configuration file (**/etc/elasticsearch/elasticsearch.yml**) and add the following to the end of the file. The **username** and **password** should be the one used to connect to Couchbase Server.

	```yaml
	couchbase.username: <USERNAME>
	couchbase.password: <PASSWORD>
	couchbase.maxConcurrentRequests: 1024
	```

6. Start Elasticsearch

	```bash
	$ /etc/init.d/elasticsearch start
	```

7. Still in the **/usr/share/elasticsearch** directory, configure the Elasticsearch plugin with the following curl command (Couchbase Server must be running).

	```bash
	$ curl -X PUT http://localhost:9200/_template/couchbase -d @plugins/transport-couchbase/couchbase_template.json
	```

	When successful, the configuration-routine provides the following response: `{"acknowledged":true}`.

8. Instantiate an Elasticsearch index to be applied by Elasticsearch to the data in a given Couchbase bucket (in this case, the **travel-sample** bucket).

	```bash
	$ curl -X PUT http://localhost:9200/travel-sample
	
	{"acknowledged":true}
	```

You have now completed all basic installation and configuration requirements for Elasticsearch, the Elasticsearch Plug-in, and Couchbase Server.