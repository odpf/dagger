# Create a job

This page contains how-to guides for creating a Dagger job and configure it.

## Prerequisites

Dagger is a stream processing framework built with Apache Flink to process/aggregate/transform protobuf data. To run a dagger in any environment you need to have the following things set up beforehand.

### `JDK and Gradle`

- Java 1.8 and gradle(5+) need to be installed to run in local mode. Follow this [link](https://www.oracle.com/in/java/technologies/javase/javase-jdk8-downloads.html) to download Java-1.8 in your setup and [this](https://gradle.org/install/) to set up gradle.

### `A Source`

Dagger currently supports 3 kinds of Data Sources. Here are the requirements for each:

##### `KAFKA_SOURCE` and `KAFKA_CONSUMER`

Both these sources use [Kafka](https://kafka.apache.org/) as the source of data. So you need to set up Kafka(1.0+) either
in a local or clustered environment. Follow this [quick start](https://kafka.apache.org/quickstart) to set up Kafka in
the local machine. If you have a clustered Kafka you can configure it to use in Dagger directly.

##### `PARQUET_SOURCE`

This source uses Parquet files as the source of data. The parquet files can be either hourly partitioned, such as
```text
root_folder
    - booking_log
        - dt=2022-02-05
            - hr=09
                * g6agdasgd6asdgvadhsaasd829ajs.parquet
                * . . . (more parquet files)
            - (...more hour folders)
        - (... more date folders)

```

or date partitioned, such as:

```text
root_folder
    - shipping_log
        - dt=2021-01-11
            * hs7hasd6t63eg7wbs8swssdasdasdasda.parquet
            * ...(more parquet files)
        - (... more date folders)

```

The file paths can be either in the local file system or in GCS bucket. When parquet files are provided from GCS bucket,
Dagger will require a `core_site.xml` to be configured in order to connect and read from GCS. A sample `core_site.xml` is
present in dagger and looks like this:
```xml
<configuration>
  <property>
    <name>google.cloud.auth.service.account.enable</name>
    <value>true</value>
  </property>
  <property>
    <name>google.cloud.auth.service.account.json.keyfile</name>
    <value>/Users/dummy/secrets/google_service_account.json</value>
  </property>
  <property>
    <name>fs.gs.requester.pays.mode</name>
    <value>CUSTOM</value>
    <final>true</final>
  </property>
  <property>
    <name>fs.gs.requester.pays.buckets</name>
    <value>my_sample_bucket_name</value>
    <final>true</final>
  </property>
  <property>
    <name>fs.gs.requester.pays.project.id</name>
    <value>my_billing_project_id</value>
    <final>true</final>
  </property>
</configuration>
```
You can look into the official [GCS Hadoop Connectors](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md)
documentation to know more on how to edit this xml as per your needs.

#### `Flink [optional]`

- Dagger uses [Apache Flink](https://flink.apache.org/) as the underlying framework for distributed stream processing. The current version of Dagger uses [Flink-1.9](https://ci.apache.org/projects/flink/flink-docs-release-1.9/).

- For distributed stream processing Flink could be configured either on a standalone cluster or in one of the supported resource managers(like Yarn, Kubernetes or docker-compose). To know more about deploying Flink on production read the [deployment section](./guides/deployment.md).

- If you want to run dagger in the local machine on a single process, you don't need to install Flink. The following Gradle command should be able to do so.

```
$ ./gradlew dagger-core:runFlink
```

- To run the Flink jobs in the local machine with java jar and local properties run the following commands.

```sh
# Creating a fat jar
$ ./gradlew :dagger-core:fatJar

# Running the jvm process
$ java -jar dagger-core/build/libs/dagger-core-<dagger-version>-fat.jar ConfigFile=<filepath>
```

#### `Protobuf Schema`

- Dagger exclusively supports [protobuf](https://developers.google.com/protocol-buffers) encoded data. That is, for a
  source reading from Kafka, Dagger consumes protobuf data from Kafka topics and does the processing. For a source reading
  from Parquet Files, dagger uses protobuf schema to parse the Row Group. When pushing the results to a sink, Dagger produces
  data as per the output protobuf schema to a Kafka topic(when the sink is Kafka).
- When using Kafka as a source, you can push data to a Kafka topic as per protobuf format using any of the Kafka client
  libraries. You can follow this [tutorial](https://www.conduktor.io/how-to-produce-and-consume-protobuf-records-in-apache-kafka/).
- For all kinds of sources, you need to define the
  [java compiled protobuf schema](https://developers.google.com/protocol-buffers/docs/javatutorial) in the classpath or
  use our in-house schema registry tool like [Stencil](https://github.com/odpf/stencil) to let dagger know about the data
  schema. Stencil is an event schema registry that provides an abstraction layer for schema handling, schema caching, and
  dynamic schema updates. [These configurations](../reference/configuration.md#schema-registry) needs to be set if you are
  using stencil for proto schema handling.

#### `Sinks`

- The current version of dagger supports Log, BigQuery, InfluxDB and Kafka and as supported sinks to push the data after processing. You need to set up the desired sinks beforehand so that data can be pushed seamlessly.

  ##### `Influx Sink`

  - [InfluxDB](https://github.com/influxdata/influxdb) is an open-source time-series database with a rich ecosystem. Influx sink allows users to analyze the data in real-time. With tools like [Grafana](https://grafana.com/), we can even visualize the data and create real-time dashboards to monitor different metrics.
  - You need to set up an Opensourced version of InfluxDB to use it as a sink. Follow [this](https://docs.influxdata.com/influxdb/v1.8/introduction/install/) to get started with the influx. We currently support InfluxDB-1.0+ in the dagger.
  - If you want to visualize the data pushed to influxDB, you have to set up grafana and add the configured influxDB as a data source.

  ##### `Kafka Sink` :

  - With Kafka sink dagger pushes the processed data as protobuf to a Kafka topic.
  - If you have a Kafka cluster set up you are good to run a Kafka sink dagger. Enable auto topic creation in Kafka or create a Kafka topic beforehand to push the data.

  ##### `BigQuery Sink` :

  - Datatype Protobuf -
    Bigquery Sink for dagger will create the BigQuery table and the dataset when they do not exist already.
    It will update the BigQuery table schema based on the latest protobuf schema. It also translates Protobuf messages into BigQuery records and inserts them into the BigQuery tables.

  - Datatype JSON -
    Currently we support dynamic schema by inferring from the incoming JSON data; so the BigQuery schema is updated by taking a diff of fields in the JSON data and the actual table fields.
    Currently, we only support string data type for the fields, so all the incoming JSON data values are converted to string type, except for metadata columns and the partition key.
## Common Configurations

- These configurations are mandatory for dagger creation and are sink independent. Here you need to set configurations such as the source details, the protobuf schema class, the SQL query to be applied on the streaming data, etc. In local execution, they would be set inside [`local.properties`](https://github.com/odpf/dagger/blob/main/dagger-core/env/local.properties) file. In the clustered environment they can be passed as job parameters to the Flink exposed job creation API.
- Configuration for a given schema involving a single source is consolidated as a Stream. In daggers, you can configure one or more streams for a single job. To know how to configure a stream based on a source, check [here](../reference/configuration.md#streams)
- The `FLINK_JOB_ID` defines the name of the flink job. `ROWTIME_ATTRIBUTE_NAME` is the key name of [row time attribute](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/concepts/time_attributes/) required for stream processing.
- In clustered mode, you can set up the `parallelism` configuration for distributed processing.
- Read more about the mandatory configurations [here](../reference/configuration.md).

```properties
STREAMS = [{
        "SOURCE_KAFKA_TOPIC_NAMES": "test-topic",
        "INPUT_SCHEMA_TABLE": "data_stream",
        "INPUT_SCHEMA_PROTO_CLASS": "com.tests.TestMessage",
        "INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX": "41",
        "SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS": "localhost:9092",
        "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE": "false",
        "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET": "latest",
        "SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID": "dummy-consumer-group",
        "SOURCE_KAFKA_NAME": "local-kafka-stream",
        "SOURCE_DETAILS": [
            {
                "SOURCE_TYPE": "UNBOUNDED",
                "SOURCE_NAME": "KAFKA_CONSUMER"
            }],
        }]

FLINK_ROWTIME_ATTRIBUTE_NAME=rowtime
FLINK_JOB_ID=TestDagger
```

## Write SQL

- Dagger is built keeping SQL first philosophy in mind. Though there are other ways to define data processing in daggers like transformers and processors, SQL is always required in the dagger. You can define the SQL query on the streaming table as part of a configuration. This example is a sample SELECT star query on the about data source.

```properties
SQL_QUERY= SELECT COUNT(1) AS no_of_events, sample_field, TUMBLE_END(rowtime, INTERVAL '60' SECOND) AS window_timestamp FROM `test-table` GROUP BY TUMBLE (rowtime, INTERVAL '60' SECOND), sample_field
```

- Flink has really good native support for SQL. All the Flink supported [SQL statements](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sql.html) should be available out of the box for a dagger.
- Also you can define custom User Defined Functions(UDFs) to add your SQL logic. We have some pre-supported [UDFs](./guides/use_udf.md) as defined [here](../reference/udfs.md). Follow [this](../contribute/add_udf.md) to add your UDFs to the dagger.
- We have noted some of the sample queries [here](./guides/query_examples.md) for different use cases.

## Log Sink

- Dagger provides a log sink which simply logs the Processed Data in a readable format. A log sink dagger requires the following config to be set.

```properties
SQL_QUERY= SELECT * from `test-table`
SINK=log
```

- Log sink is mostly used for testing and debugging purpose since it just a print statement for the processed data. This is a sample message produced in the log sink after the simple query above.

```
INFO  io.odpf.dagger.core.sink.log.LogSink                            - {sample_field=81179979,sample_field_2=81179979, rowtime=2021-05-21 11:55:33.0, event_timestamp=1621598133,0}
```

## Influx Sink

- Influx sink is useful for data analytics or even for data validation while iterating over the business logic.
- For the InfluxDB sink, you have to specify the InfluxDB level information as well as an influx measurement to push the processed data.
- Showing some of the configurations essential for influx sink Dagger. Find more about them [here](../reference/configuration.md).

```properties
# === sink config ===
SINK_TYPE=influx
# === Influx config ===
INFLUX_URL=http://localhost:8086
INFLUX_USERNAME=root
INFLUX_PASSWORD=
INFLUX_DATABASE=DAGGERS
INFLUX_MEASUREMENT_NAME=concurrent_test
INFLUX_BATCH_SIZE=100
INFLUX_FLUSH_DURATION_IN_MILLISECONDS=1000
INFLUX_RETENTION_POLICY=autogen
```

- In Influx sink, add [`tag_`](https://docs.influxdata.com/influxdb/v1.8/concepts/glossary/#tag-key) prefix to use the column as a dimension. Dimensions will help you slice the data in InfluxDB-Grafana. InfluxDB tags are essentially the columns on which data are indexed. DO NOT use tag* for high cardinal data points with a lot of unique values unless you provide a filtering condition; this will create tag explosion & kill the InfluxDB. Ensure there is at least one value field present in the query(not starting with tag*).

- The data generated can be visualized with the help of tools like Grafana & chronograph.

## Kafka Sink

- Kafka sink enables aggregated data to be published to a Kafka topic in protobuf encoded format and is available for consumption by any type of consumer. After a Dagger job is deployed, the new topic is automatically created in the output Kafka cluster if auto-creation is enabled.
- Listing some of the configurations essential for Kafka sink Dagger. Find more about them [here](../reference/configuration.md).

```properties
# === sink config ===
SINK_TYPE=kafka
# === Output Kafka config ===
OUTPUT_PROTO_MESSAGE=com.tests.TestMessage
OUTPUT_KAFKA_BROKER=localhost:9092
OUTPUT_KAFKA_TOPIC=test-kafka-output
```

- Dimensions & metrics from the SELECT section in the query need to be mapped to field names in the output proto.
- Find more examples on Kafka sink SQL queries [here](./guides/query_examples.md#kafka-sink).

## Bigquery Sink

### Datatype Protobuf
Bigquery Sink has several responsibilities, first creation of bigquery table and dataset when they are not exist,
second update the bigquery table schema based on the latest protobuf schema,
third translate protobuf messages into bigquery records and insert them to bigquery tables.
Bigquery utilise Bigquery [Streaming API](https://cloud.google.com/bigquery/streaming-data-into-bigquery) to insert record into bigquery tables.

### Datatype JSON
Bigquery Sink has several responsibilities, first creation of bigquery table and dataset when they are not exist,
Currently we support dynamic schema by inferring from incoming json data; so the bigquery schema is updated by taking a diff of fields in json data and actual table fields.
Currently we only support string data type for fields, so all incoming json data values are converted to string type, Except for metadata columns and partion key.


### Bigquery Table Schema Update

### Protobuf
Bigquery Sink update the bigquery table schema on separate table update operation. Bigquery utilise [Stencil](https://github.com/odpf/stencil) to parse protobuf messages generate schema and update bigquery tables with the latest schema.
The stencil client periodically reload the descriptor cache. Table schema update happened after the descriptor caches uploaded.

### Protobuf - Bigquery Table Type Mapping

Here are type conversion between protobuf type and bigquery type :

| Protobuf Type | Bigquery Type |
| --- | ----------- |
| bytes | BYTES |
| string | STRING |
| enum | STRING |
| float | FLOAT |
| double | FLOAT |
| bool | BOOLEAN |
| int64, uint64, int32, uint32, fixed64, fixed32, sfixed64, sfixed32, sint64, sint32 | INTEGER |
| message | RECORD |
| .google.protobuf.Timestamp | TIMESTAMP |
| .google.protobuf.Struct | STRING (Json Serialised) |
| .google.protobuf.Duration | RECORD |

| Protobuf Modifier | Bigquery Modifier |
| --- | ----------- |
| repeated | REPEATED |


### Partitioning

Bigquery Sink supports creation of table with partition configuration. Currently, Bigquery Sink only supports time based partitioning.
To have time based partitioning protobuf `Timestamp` as field is needed on the protobuf message. The protobuf field will be used as partitioning column on table creation.
The time partitioning type that is currently supported is `DAY` partitioning.

### Metadata

For data quality checking purposes, sometimes some metadata need to be added on the record.
if `SINK_BIGQUERY_ADD_METADATA_ENABLED` is true then the metadata will be added.
`SINK_BIGQUERY_METADATA_NAMESPACE` is used for another namespace to add columns
if namespace is empty, the metadata columns will be added in the root level.
`SINK_BIGQUERY_METADATA_COLUMNS_TYPES` is set with kafka metadata column and their type,
An example of metadata columns that can be added for kafka records.

### Default columns for json data type
With dynamic schema for json we need to create table with some default columns, example like parition key needs to be set during creation of the table.
Example `SINK_BIGQUERY_DEFAULT_COLUMNS =event_timestamp=timestamp`
The metadata columns are added when input data contains values for the them which will result in missing fields error and will be added by the JSON error handler.

| Fully Qualified Column Name | Type | Modifier |
| --- | ----------- | ------- | 
| metadata_column | RECORD | NULLABLE |
| metadata_column.message_partition | INTEGER | NULLABLE |
| metadata_column.message_offset | INTEGER | NULLABLE |
| metadata_column.message_topic | STRING | NULLABLE |
| metadata_column.message_timestamp | TIMESTAMP | NULLABLE |
| metadata_column.load_time | TIMESTAMP | NULLABLE |

### Errors Handling

The response can contain multiple errors which will be sent to the application.

| Error Name | Generic Error Type | Description |
| --- | ----------- | ------- | 
| Stopped Error | SINK_5XX_ERROR | Error on a row insertion that happened because insert job is cancelled because other record is invalid although current record is valid |
| Out of bounds Error | SINK_4XX_ERROR | Error on a row insertion the partitioned column has a date value less than 5 years and more than 1 year in the future |
| Invalid schema Error | SINK_4XX_ERROR | Error on a row insertion when there is a new field that is not exist on the table or when there is required field on the table |
| Other Error | SINK_UNKNOWN_ERROR | Uncategorized error |

### Google Cloud Bigquery IAM Permission

Several IAM permission is required for bigquery sink to run properly,

* Create and update Table
  * bigquery.tables.create
  * bigquery.tables.get
  * bigquery.tables.update
* Create and update Dataset
  * bigquery.datasets.create
  * bigquery.datasets.get
  * bigquery.datasets.update
* Stream insert to Table
  * bigquery.tables.updateData

Further documentation on bigquery IAM permission [here](https://cloud.google.com/bigquery/streaming-data-into-bigquery).

## Advanced Data Processing

- Dagger's inbuilt SQL enables users to do some complex stream processing like aggregations, joins, windowing, union etc. Find more of the sample queries [here](./guides/query_examples.md).
- Dagger supports an internal framework called processors to support some of the more complex Data processing beyond SQL. Some of them are,
  - Custom Java code injection [Transformers](./guides/use_transformer.md)
  - [External source support](../advance/post_processor.md#external-post-processor) for data enrichment
  - [Large Windowed Streaming query](../advance/longbow.md)

`Note`: _You can configure a lot of other advanced configurations related to stream processing(like watermarks) and related to the dagger. Please follow the [configuration](../reference/configuration.md) section._

`Note`: _To add a new feature like support for new source/sinks/postprocessors/udfs please follow the [contribution guidelines](../contribute/contribution.md)._
