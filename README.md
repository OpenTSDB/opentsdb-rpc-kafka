       ___                 _____ ____  ____  ____
      / _ \ _ __   ___ _ _|_   _/ ___||  _ \| __ )
     | | | | '_ \ / _ \ '_ \| | \___ \| | | |  _ \
     | |_| | |_) |  __/ | | | |  ___) | |_| | |_) |
      \___/| .__/ \___|_| |_|_| |____/|____/|____/
           |_|    The modern time series database.

[![Build Status](https://travis-ci.org/OpenTSDB/opentsdb-rpc-kafka.svg?branch=master)](https://travis-ci.org/OpenTSDB/opentsdb-rpc-kafka) [![Coverage Status](https://coveralls.io/repos/github/OpenTSDB/opentsdb-rpc-kafka/badge.svg?branch=master)](https://coveralls.io/github/OpenTSDB/opentsdb-rpc-kafka?branch=master)

# Kafka RPC Plugin

This plugin allows OpenTSDB to consume messages from a Kafka cluster and write them directly to storage, bypassing the Telnet style or HTTP APIs. It includes a Storage Exception Handler plugin that will post messages back to a Kafka queue if writing to storage fails.

**NOTE:** This branch is compatible with OpenTSDB 2.4.x. For 2.3.x please use the `2.3` branch from Git.

## Installation

1. Download the source code and run ``mvn package -Pshaded`` to create the shaded jar in the ``target/`` directory. Copy this file to your OpenTSDB plugin directory as defined in the opentsdb config via ``tsd.core.plugin_path`` (Note that without the `-P` flag it will simply build a basic jar without the Kafka libraries. Those will need to be on your class path.).
1. Setup the appropriate Kafka topics and partitions. More on that later.
1. Add configuration settings to your ``opentsdb.conf`` file as described later on.
1. Restart the TSD and make sure the plugin was loaded and associated with the proper ID. E.g. look in the logs for lines like:

```
2017-07-10 23:08:58,264 INFO  [main] RpcManager: Successfully initialized plugin [net.opentsdb.tsd.KafkaRpcPlugin] version: 2.4.0
2017-07-10 23:08:57,790 INFO  [main] TSDB: Successfully initialized storage exception handler plugin [net.opentsdb.tsd.KafkaStorageExceptionHandler] version: 2.4.0

```

## Usage

The plugin can accept various message formats using classes implementing the `net.opentsdb.data.deserializers.Deserializer` interface. A `Deserializer` must be defined per consumer group and all messages in the topics must be of the same format. 

For default JSON formatted messages, use the `net.opentsdb.data.deserializers.JSONDeserializer`. Each JSON message *must* include a ``type`` field with the value being one of the following:

* **Metric** A single numeric measurement.
* **Aggregate** A single numeric measurement that may be a pre-aggregate, a rolled up data point or both.
* **Histogram** A single histogram measurement.

Each message is similar to the HTTP JSON messages in the OpenTSDB API with the addition of the ``type`` field so that the JSON deserializer can figure out what the message contains.

The deserialization class must return a list of typed objects as defined below (with JSON provided as an example).

###Metric

The metric message appears as follows:

```javascript
{
	"type": "Metric",
	"metric": "sys.cpu.user",
	"tags": {
		"host": "web01"
	},
	"timestamp": 1492641000,
	"value": 42
}

```

For field information, see [/api/put] (http://opentsdb.net/docs/build/html/api_http/put.html).

###Aggregate

Aggregate messages are the same as those documented in [/api/rollup] (http://opentsdb.net/docs/build/html/api_http/rollup.html).

###Histogram

Histogram messages are documented at [/api/histogram] (http://opentsdb.net/docs/build/html/api_http/histogram.html).

## Configuration

The following properties can be stored in the ``opentsdb.conf`` file:

|Property|Type|Required|Description|Default|Example|
|--------|----|--------|-----------|-------|-------|
|tsd.rpc.plugins|String|Required|The full class name of the plugin. This must be ``net.opentsdb.tsd.KafkaRpcPlugin``||net.opentsdb.tsd.KafkaRpcPlugin|
|KafkaRpcPlugin.kafka.zookeeper.connect|String|Required|The comma separated list of zookeeper hosts and ports used by the Kafka cluster.||localhost:2181|
|KafkaRpcPlugin.groups|String|Required|A comma separated list of one or more consumer group names.||TsdbConsumer,TsdbRequeueConsumer|
|KafkaRpcPlugin.\<GROUPNAME\>.topics|String|Required|A comma separated list of one or more topics for the ``<GROUPNAME>`` to consume from.||TSDB_1,TSDB_2|
|KafkaRpcPlugin.\<GROUPNAME\>.consumerType|String|Required|The type of messages written to the queue. TODO. For now, leave it as ``raw``||raw|
|KafkaRpcPlugin.\<GROUPNAME\>.deserializer|String|Required|The deserialization class to use for parsing messages from the Kafka topic.||net.opentsdb.data.deserializers.JSONDeserializer|
|KafkaRpcPlugin.\<GROUPNAME\>.rate|Integer|Required|How many messages per second to throttle the total of consumer threads at for the consumer group||250000|
|KafkaRpcPlugin.\<GROUPNAME\>.threads|Integer|Required|The number of consumer threads to create per group||4|
|tsd.http.rpc.plugins|String|Optional|A comma separated list of HTTP RPC plugins to load. Included with this package is a plugin that allows for fetching stats from the Kafka plugin as well as viewing or modifying the write rate during runtime.||net.opentsdb.tsd.KafkaHttpRpcPlugin|
|tsd.core.storage\_exception\_handler.enable|Boolean|Optional|Whether or not to enable the storage exception handler plugin.|false|true|
|tsd.core.storage\_exception\_handler.plugin|String|Optional|The full class of the storage exception handler plugin.||net.opentsdb.tsd.KafkaStorageExceptionHandler|
|KafkaRpcPlugin.kafka.metadata.broker.list|String|Optional|The comma separated list of Kafka brokers and ports used to write messages to for the storage exception handler plugin||localhost:9092|
|KafkaRpcPlugin.seh.topic.default|String|Optional|The topic used to write messages to for the storage exception handler.||TSDB_Requeue|

Note the ``KafkaRpcPlugin.groups`` and ``<GROUP_NAME>`` entries above. Kafka consumers belong to a particular group. The Kafka RPC plugin can launch multiple groups consuming from multiple topics so that OpenTSDB messages can be organized by type or source for more efficient control over rate limits and priorities. When setting the ``KafkaRpcPlugin.groups`` value, make sure you have a complete set of ``KafkaRpcPlugin.<GROUP_NAME>.*`` parameters per group or initialization will fail.
