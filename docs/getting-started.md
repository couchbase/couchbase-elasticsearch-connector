---
---

A Couchbase-Elasticsearch data-replication system consists of three principal components:

- a **Couchbase Server-cluster** of one or more nodes
- an **Elasticsearch cluster** of one or more nodes
- the **Elasticsearch Transport Plug-in**, installed in the Elasticsearch environment.

This section provides a step-by-step procedure, whereby the three components can be installed, configured, and run.

## Installation

1. Download the Elasticsearch 2.4.0 package for Ubuntu.

	```bash
	$ wget https://download.elastic.co/elasticsearch/elasticsearch-2.4.0.deb
	```

2. Install the package.

	```bash
	$ dpkg -i elasticsearch-2.4.0.deb
	```

3. Navigate to the Elasticsearch installation directory.

	```bash
	$ cd /usr/share/elasticsearch
	```

4. Download and install the plug-in package.

	```bash
	bin/plugin install https://github.com/couchbaselabs/elasticsearch-transport-couchbase/releases/download/2.2.4.0-update1/elasticsearch-transport-couchbase-2.2.4.0-update1.zip
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

7. Configure an Elasticsearch index-template

	```bash
	$ curl -XPUT http://localhost:9200/_template/couchbase -d @plugins/transport-couchbase/couchbase_template.json
	```

	When successful, the configuration-routine provides the following response: `{"acknowledged":true}`.

8. Instantiate an elasticsearch index to be applied by Elasticsearch to data from a particular Couchbase bucket (in this case, beer-sample, which you previously used in your set-up of Couchbase Server).

	```bash
	$ curl -XPUT http://localhost:9200/beer-sample
	```
	
	When index-creation is successful, a further `{"acknowledged":true}` response is provided.

You have now completed all basic installation and configuration requirements for Elasticsearch, the Elasticsearch Plug-in, and Couchbase Server. (Note that more advanced configuration options are described later in this document).