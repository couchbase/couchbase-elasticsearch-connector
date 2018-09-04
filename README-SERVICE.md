>This document shows how to get started with the **Connector Service** flavor of the Couchbase Elasticsearch Connector.

## Pre-Release Disclaimer

This software has not yet been released. Features, requirements, documentation,
and supported versions may change before release.

Please [file an issue](https://github.com/couchbaselabs/couchbase-elasticsearch-connector/issues)
if you run into a problem or have an idea for a feature request. Thanks!

## Connector Service Requirements

* Couchbase Server 5.0 or newer.
* Elasticsearch 5 or newer (6.x recommended).
* Java 8 or newer (Oracle or OpenJDK)

You might also want the [elasticsearch-head](https://github.com/mobz/elasticsearch-head) plugin for
Google Chrome so you can easily see the documents in Elasticsearch.

## Installation

Connector Service distributions are available from the [Releases](https://github.com/couchbaselabs/elasticsearch-transport-couchbase/releases) page.
Download the latest `couchbase-elasticsearch-connector-<version>.zip`. (Ignore the `elasticsearch-transport-couchbase` files; those are for the plugin flavor.) 

Unzip the archive. These instructions refer to the unzipped directory as `$CBES_HOME`.

Add `$CBES_HOME/bin` to your `PATH`.
 
Copy `$CBES_HOME/config/example-connector.toml` to
`$CBES_HOME/config/default-connector.toml`.

> The connector commands get their configuration from `$CBES_HOME/config/default-connector.toml` by default.
You can tell them to use a different config file with the `--config <file>` command line option.

Take a moment to browse the settings available in `default-connector.toml`. Make sure
the Couchbase and Elasticsearch credentials and hostnames match your environment.
Note that the passwords are stored separately in the `$CBES_HOME/secrets` directory.

If you're using Elasticsearch 5.x, replace all instances of the type name `_doc` with something that
doesn't have a leading underscore.

The sample config will replicate documents from the Couchbase `travel-sample` bucket. 
Go ahead and
[install the sample buckets](https://developer.couchbase.com/documentation/server/current/settings/install-sample-buckets.html)
now if you haven't already.

## Starting a connector process

Run this command:

    cbes

The connector should start copying documents from the `travel-sample` bucket into Elasticsearch.

## Monitoring a connector process

Metrics are exposed over HTTP:

    http://localhost:31415/metrics?pretty


## Stopping a connector process

A connector process will shut down gracefully in response to an interrupt signal
(ctrl-c, or `kill -s INT <pid>`).


## Connector process groups

Distributed connector processes can share the work of replicating a bucket.
Install the connector distribution on multiple machines.
Make sure the configuration is identical, except for the `memberNumber` config key
which must be unique within the group.


## Managing checkpoints

The connector periodically saves its replication state by writing metadata documents to the
Couchbase bucket. These documents have IDs starting with "`_connector:cbes:`".

Command line tools are provided to manage the replication checkpoint.

> WARNING: You must stop all connector processes before modifying the replication checkpoint.

#### Saving the current replication state

To create a backup of the current state:

    cbes-checkpoint-backup --output <checkpoint.json>

This will create a checkpoint document on the local filesystem. On Linux, to include a timestamp in the filename: 

    cbes-checkpoint-backup --output checkpoint-$(date -u +%Y-%m-%dT%H:%M:%SZ).json

This command is safe to use while the connector is running,
and can be triggered from a cron job to create periodic backups. 


#### Reverting to a saved checkpoint

If you want to rewind the event stream and re-index documents starting from a saved checkpoint,
first stop all running connector processes in the connector group.
Then run:

    cbes-checkpoint-restore --input <checkpoint.json>

The next time you run the connector, it will resume from the checkpoint you just restored. 

#### Resetting the connector

If you want to discard all replication state and start streaming from the beginning,
first stop all of the connector processes, then run:

    cbes-checkpoint-clear
    
Or, if you want to reset the connector so it starts from the
current state of the bucket:

    cbes-checkpoint-clear --catch-up


# Migrating from the Plugin

### Document structure and metadata

By default, Elasticsearch documents created by the Connector Service have the same
structure and metadata as those created by the plugin, with the addition
of new metadata fields that can be used to build a Couchbase Mutation Token.

##### Metadata Fields

|  Name      | Datatype | New in 4.0  
------------:|:---------|:-----:
| vbucket    | integer  | ✓ 
| vbuuid     | long     | ✓
| seqno      | long     | ✓ 
| revSeqno   | long     | ✓
| cas        | long     | ✓
| lockTime   | integer  | ✓
| rev        | string   | 
| flags      | integer  | 
| expiration | integer  | 
| id         | string   | 


### Replication state

The Connector Service stores its replication state in a way that is incompatible
with the plugin. If re-streaming all of the documents from Couchbase is not
an option, you can use the new checkpoint management tools to create a
checkpoint from the current state of the bucket (with `cbes-checkpoint-clear --catch-up`).
Allow the plugin to finish replicating documents up to or past that state,
then uninstall it and start the Connector Service.


# Building the connector from source

The connector distribution may be built from source with the command:
    
    ./gradlew build

The distribution archive will be generated under `build/distributions`.
During development, it might be more convenient to run:

    ./gradlew installDist
    
which creates `build/install/couchbase-elasticsearch-connector` as a `$CBES_HOME` directory.    

### IntelliJ setup  
Because the project uses annotation processors, some [fiddly setup](INTELLIJ-SETUP.md) is required when importing the project into IntelliJ.
