# Configurations

This page contains references for all the application configurations for Dagger.

## Table of Contents

* [Generic](configuration.md#generic)
* [Influx Sink](configuration.md#influx-sink)
* [Kafka Sink](configuration.md#kafka-sink)
* [BigQuery Sink](configuration.md#bq-sink)
* [Schema Registry](configuration.md#schema-registry)
* [Flink](configuration.md#flink)
* [Darts](configuration.md#darts)
* [Longbow](configuration.md#longbow)
* [PreProcessor](configuration.md#preprocessor)
* [PostProcessor](configuration.md#postprocessor)
* [Telemetry](configuration.md#telemetry)
* [Python Udfs](configuration.md#python-udfs)


### Generic

All the sink types of Dagger require the following variables to be set:

#### `STREAMS`

Dagger can run on multiple streams, so STREAMS config can con consist of multiple streams. Multiple streams could be given in a comma-separated format.

For each stream, these following variables need to be configured: 

#### `SOURCE_DETAILS`
Defines the type of source to be used as well as its boundedness. This is an ordered JSON array, with each JSON structure 
containing two fields: `SOURCE_NAME` and `SOURCE_TYPE`. As of the latest release, only one source can be configured per stream and 
hence arity of this array cannot be more than one. 

| **JSON Field Name**|**Field Name Description**|**Data Type**|**Data Type Description**|
|--|--|--|--|
|`SOURCE_TYPE`| Defines the boundedness of the source |**ENUM** [`BOUNDED`, `UNBOUNDED`] |<ul><li>`BOUNDED` is a data source type which is known to be finite and has a fixed start and end point. Once the dagger job is created and running, new additions of data to this source will not be processed.</li><li> `UNBOUNDED` is a data source with a fixed starting point but theoretically infinite end point. New data added will be processed even after dagger has been started and is running.</li></ul>|
|`SOURCE_NAME`|Defines the formal, registered name of the source in Dagger|**ENUM**[`KAFKA_SOURCE`, `PARQUET_SOURCE`, `KAFKA_CONSUMER`]|<ul><li>`KAFKA_SOURCE` is an `UNBOUNDED` data source type using Apache Kafka as the source.</li><li>`PARQUET_SOURCE` is a `BOUNDED` data source type using Parquet Files present in GCS Buckets as the source.</li><li>`KAFKA_CONSUMER` is a `BOUNDED` source type built on deprecated [FlinkKafkaConsumer](https://nightlies.apache.org/flink/flink-docs-release-1.14/api/java/org/apache/flink/streaming/connectors/kafka/FlinkKafkaConsumer.html).</li></ul>|

* Example value: `[{"SOURCE_TYPE": "UNBOUNDED","SOURCE_NAME": "KAFKA_CONSUMER"}]`
* Type: `required`

##### `SOURCE_KAFKA_TOPIC_NAMES`

Defines the list of Kafka topics to consume from. To consume from multiple topics, you need to add `|` as separator for each topic.

* Example value: `test-topic1|test-topic2`
* Type: `required` only when `KAFKA_CONSUMER` or `KAFKA_SOURCE` is configured in `SOURCE_DETAILS`

##### `INPUT_SCHEMA_TABLE`

Defines the table name for the stream. `FLINK_SQL_QUERY` will get executed on this table name.

* Example value: `data_stream`
* Type: `required`

##### `INPUT_SCHEMA_PROTO_CLASS`

Defines the schema proto class of input data.

* Example value: `com.tests.TestMessage`
* Type: `required`

##### `INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX`

Defines the field index of event timestamp from the input proto class that will be used. The field index value can be obtained from `.proto` file.

* Example value: `41`
* Type: `required`

##### `SOURCE_KAFKA_CONFIG_BOOTSTRAP_SERVERS`

Defines the bootstrap server of Kafka brokers to consume from. Multiple Kafka brokers could be given in a comma-separated format.

* Example value: `localhost:9092`
* Type: `required` only when `KAFKA_CONSUMER` or `KAFKA_SOURCE` is configured in `SOURCE_DETAILS`

##### `SOURCE_KAFKA_CONFIG_AUTO_COMMIT_ENABLE`

Enable/Disable Kafka consumer auto-commit. Find more details on this config [here](https://kafka.apache.org/documentation/#consumerconfigs_enable.auto.commit).

* Example value: `false`
* Type: `optional`
* Default value: `false`

##### `SOURCE_KAFKA_CONFIG_AUTO_OFFSET_RESET`

Defines the Kafka consumer offset reset policy. Find more details on this config [here](https://kafka.apache.org/documentation/#consumerconfigs_auto.offset.reset).

* Example value: `latest`
* Type: `optional`
* Default value: `latest`

##### `SOURCE_KAFKA_CONFIG_GROUP_ID`

Defines the Kafka consumer group ID for Dagger deployment. Find more details on this config [here](https://kafka.apache.org/documentation/#consumerconfigs_group.id).

* Example value: `dummy-consumer-group`
* Type: `required` only when `KAFKA_CONSUMER` or `KAFKA_SOURCE` is configured in `SOURCE_DETAILS`

##### `SOURCE_KAFKA_NAME`

Defines a name for the Kafka cluster. It's a logical way to name your Kafka clusters.This helps with identifying different kafka cluster the job might be interacting with.

* Example value: `local-kafka-stream`
* Type: `required` only when `KAFKA_CONSUMER` or `KAFKA_SOURCE` is configured in `SOURCE_DETAILS`

#### `SOURCE_PARQUET_FILE_PATHS`

Defines the array of date partitioned or hour partitioned file path URLs to be processed by Parquet Source. These can be
either local file paths such as `/Users/dummy_user/booking_log/dt=2022-01-23/` or GCS file path URLs.

* Example value:  `["gs://my-sample-bucket/booking-log/dt=2022-01-23/", "gs://my-sample-bucket/booking-log/dt=2021-01-23/"]`
* Type: `required` only when `PARQUET_SOURCE` is configured in `SOURCE_DETAILS`

Note: 
1. Each file path in the array can be either a fully qualified file path, 
for example `gs://my-sample-bucket/booking-log/dt=2021-01-23/my_file.parquet` or it can be a directory path, 
for example `gs://my-sample-bucket/booking-log/`. For the latter, Dagger upon starting will first do a recursive search 
for all files under the `booking-log` directory. If 
[SOURCE_PARQUET_FILE_DATE_RANGE](configuration.md#source_parquet_file_date_range) is configured, it will only add those
files as defined by the range into its internal index for processing and skip the others. If not configured, all the 
discovered files are processed.

#### `SOURCE_PARQUET_READ_ORDER_STRATEGY`

Defines the ordering in which files discovered from `SOURCE_PARQUET_FILE_PATHS` will be processed. Currently, this takes 
just one possible value: `EARLIEST_TIME_URL_FIRST`, however more strategies can be added later.
In `EARLIEST_TIME_URL_FIRST` strategy, Dagger will extract chronological information from the GCS file path URLs and then
begin to process them in the order of ascending timestamps.

* Example value: `EARLIEST_TIME_URL_FIRST`
* Type: `optional`
* Default value: `EARLIEST_TIME_URL_FIRST`

#### `SOURCE_PARQUET_FILE_DATE_RANGE`

Defines the time range which, if present, will be used to decide which files to add for processing post discovery from `SOURCE_PARQUET_FILE_PATHS`.
Each time range consists of two ISO format timestamps, start time and end time, separated by a comma. Multiple time range
intervals can also be provided separated by semicolon. 

* Example value: `2022-05-08T00:00:00Z,2022-05-08T23:59:59Z;2022-05-10T00:00:00Z,2022-05-10T23:59:59Z`
* Type: `optional`

Please follow these guidelines when setting this configuration:
1. Both the start and end time range are inclusive. For example, 
   1. For an hour partitioned GCS folder structure,`2022-05-08T00:00:00Z,2022-05-08T10:00:00Z` will imply all files of hour 00, 01,..., 10 will be processed.
   2. For a date partitioned GCS folder structure, `2022-05-08T00:00:00Z,2022-05-10T23:59:59Z` will imply all files of 8th, 9th and 10th May will be processed.
2. For a date partitioned GCS folder, it is only the date component that is taken into consideration for selecting which files to process and the time component is ignored. However,
for start-time, ensure that the time component is always set to 00:00:00. Otherwise, results might be unexpected and there will be data drop. For example, for a date partitioned GCS bucket folder, 
   1. `2022-05-08T00:00:00Z,2022-05-08T10:00:00Z` is a valid config. All files for 8th May will be processed.
   2. `2022-05-08T00:00:01Z,2022-05-08T10:00:00Z` is not a valid config and will cause the entire data for 2022-05-08 to be skipped.

##### Sample STREAMS Configuration using KAFKA_CONSUMER as the data source :
```
STREAMS = [
 {
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
     }
   ]
 }
]
```

##### Sample STREAMS Configuration using PARQUET_SOURCE as the data source :
```
STREAMS = [
 {
   "INPUT_SCHEMA_TABLE": "data_stream",
   "INPUT_SCHEMA_PROTO_CLASS": "com.tests.TestMessage",
   "INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX": "41",
   "SOURCE_PARQUET_FILE_PATHS": [
   "gs://p-godata-id-mainstream-bedrock/carbon-offset-transaction-log/dt=2022-02-05/",
   "gs://p-godata-id-mainstream-bedrock/carbon-offset-transaction-log/dt=2022-02-03/"
   ],
   "SOURCE_PARQUET_FILE_DATE_RANGE":"2022-02-05T00:00:00Z,2022-02-05T10:59:59Z;2022-02-03T00:00:00Z,2022-02-03T20:59:59Z"
   "SOURCE_PARQUET_READ_ORDER_STRATEGY": "EARLIEST_TIME_URL_FIRST",
   "SOURCE_DETAILS": [
     {
       "SOURCE_TYPE": "BOUNDED",
       "SOURCE_NAME": "PARQUET_SOURCE"
     }
   ]
 }
]


```

#### `SINK_TYPE`

Defines the Dagger sink type. At present, we support `log`, `influx`, `kafka`

* Example value: `log`
* Type: `required`
* Default value: `influx`

#### `FLINK_JOB_ID`

Defines the dagger deployment name

* Example value: `SQL Flink job`
* Type: `optional`
* Default value: `SQL Flink job`

#### `FLINK_ROWTIME_ATTRIBUTE_NAME`

Defines the time attribute field name on the data stream. Find more details on this config [here](../concepts/basics.md#rowtime).

* Example value: `rowtime`
* Type: `required`

#### `FUNCTION_FACTORY_CLASSES`

Defines the factory class of the UDF. Multiple factory classes could be given in a comma-separated format.

* Example value: `io.odpf.dagger.functions.udfs.factories.FunctionFactory`
* Type: `Optional`
* Default value: `io.odpf.dagger.functions.udfs.factories.FunctionFactory`

#### `SQL_QUERY`

Defines the SQL query to get the data from the data stream.

* Example value: `SELECT * from data_stream`
* Type: `required`

#### `SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE`

Enable/Disable to consume large messages from Kafka. by default, it's configuration using the default `max.partition.fetch.bytes` Kafka config. If set to enable, will set the `max.partition.fetch.bytes`=`5242880`.

* Example value: `false`
* Type: `optional`
* Default value: `false`

### Influx Sink

An Influx sink Dagger \(`SINK_TYPE`=`influx`\) requires the following variables to be set along with Generic ones.

#### `SINK_INFLUX_URL`

InfluxDB URL, it's usually the hostname followed by port.

* Example value: `http://localhost:8086`
* Type: `required`

#### `SINK_INFLUX_USERNAME`

Defines the username to connect to InfluxDB.

* Example value: `root`
* Type: `required`

#### `SINK_INFLUX_PASSWORD`

Defines the password to connect to InfluxDB.

* Example value: `root`
* Type: `required`

#### `SINK_INFLUX_DB_NAME`

Defines the InfluxDB database name.

* Example value: `DAGGER`
* Type: `required`

#### `SINK_INFLUX_MEASUREMENT_NAME`

Defines the InfluxDB measurement name. Find more details on using this config [here](https://docs.influxdata.com/influxdb/v1.8/concepts/glossary/#measurement).

* Example value: `concurrent_test`
* Type: `required`

#### `SINK_INFLUX_RETENTION_POLICY`

Defines the InfluxDB retention policy. Find more details on using this config [here](https://docs.influxdata.com/influxdb/v1.8/concepts/glossary/#retention-policy-rp).

* Example value: `autogen`
* Type: `optional`
* Default value: `autogen`

#### `SINK_INFLUX_BATCH_SIZE`

Defines the InfluxDB batch size. Find more details on using this config [here](https://docs.influxdata.com/influxdb/v1.8/concepts/glossary/#batch).

* Example value: `100`
* Type: `optional`
* Default value: `0`

#### `SINK_INFLUX_FLUSH_DURATION_MS`

Defines the InfluxDB flush duration in milliseconds. Find more details on using this config [here](https://docs.influxdata.com/influxdb/v1.8/concepts/glossary/#wal-write-ahead-log).

* Example value: `1000`
* Type: `optional`
* Default value:  `0`

### Kafka Sink

A Kafka sink Dagger \(`SINK_TYPE`=`kafka`\) requires the following variables to be set along with Generic ones.

#### `SINK_KAFKA_BROKERS`

Defines the list of Kafka brokers sink. 

* Example value: `localhost:9092`
* Type: `required`

#### `SINK_KAFKA_TOPIC`

Defines the topic of Kafka sink.

* Example value: `test-kafka-output`
* Type: `required`

#### `SINK_KAFKA_PROTO_KEY`

Defines the proto class key of the data to Kafka sink.

* Example value: `com.tests.OutputKey`
* Type: `required`

#### `SINK_KAFKA_PROTO_MESSAGE`

Defines the proto class to which the message will get serialized and will be sent to a kafka topic after processing.

* Example value: `com.tests.OutputMessage`
* Type: `required`

#### `SINK_KAFKA_STREAM`

Defines a name for the Kafka cluster. It's a logical way to name your Kafka clusters.This helps with identifying different kafka cluster the job might be interacting with.

* Example value: `output-stream-name`
* Type: `required`

#### `SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE`

Enable/Disable to produce large messages to Kafka. by default, it's configuration using the default `max.request.size` Kafka config. If set to enable, will set the `max.request.size`=`20971520` and `compression.type`=`snappy`.
 
* Example value: `false`
* Type: `optional`
* Default value: `false`

### BigQuery Sink

A Bigquery sink Dagger \(`SINK_TYPE`=`bigquery`\) requires the following variables to be set along with Generic ones

#### `SINK_BIGQUERY_GOOGLE_CLOUD_PROJECT_ID`

Contains information of google cloud project id location of the bigquery table where the records need to be inserted. Further documentation on google cloud [project id](https://cloud.google.com/resource-manager/docs/creating-managing-projects).

- Example value: `gcp-project-id`
- Type: `required`

#### `SINK_BIGQUERY_TABLE_NAME`

The name of bigquery table. Here is further documentation of bigquery [table naming](https://cloud.google.com/bigquery/docs/tables).

- Example value: `user_profile`
- Type: `required`

#### `SINK_BIGQUERY_DATASET_NAME`

The name of dataset that contains the bigquery table. Here is further documentation of bigquery [dataset naming](https://cloud.google.com/bigquery/docs/datasets).

- Example value: `customer`
- Type: `required`

#### `SINK_BIGQUERY_DATASET_LABELS`

Labels of a bigquery dataset, key-value information separated by comma attached to the bigquery dataset. This configuration define labels that will be set to the bigquery dataset. Here is further documentation of bigquery [labels](https://cloud.google.com/bigquery/docs/labels-intro).

- Example value: `owner=data-engineering,granurality=daily`
- Type: `optional`

#### `SINK_BIGQUERY_TABLE_LABELS`

Labels of a bigquery table, key-value information separated by comma attached to the bigquery table. This configuration define labels that will be set to the bigquery dataset. Here is further documentation of bigquery [labels](https://cloud.google.com/bigquery/docs/labels-intro).

- Example value: `owner=data-engineering,granurality=daily`
- Type: `optional`

#### `SINK_BIGQUERY_TABLE_PARTITIONING_ENABLE`

Configuration for enable table partitioning. This config will be used for provide partitioning config when creating the bigquery table.
Bigquery table partitioning config can only be set once, on the table creation and the partitioning cannot be disabled once created. Changing this value of this config later will cause error when dagger trying to update the bigquery table.
Here is further documentation of bigquery [table partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables).

- Example value: `true`
- Type: `required`
- Default value: `false`

#### `SINK_BIGQUERY_TABLE_PARTITION_KEY`

Define protobuf/bigquery field name that will be used for bigquery table partitioning. only protobuf `Timestamp` field, that later converted into bigquery `Timestamp` column that is supported as partitioning key.
Currently, this sink only support `DAY` time partitioning type.
Here is further documentation of bigquery [column time partitioning](https://cloud.google.com/bigquery/docs/creating-partitioned-tables#console).

- Example value: `event_timestamp`
- Type: `required`

#### `SINK_BIGQUERY_ROW_INSERT_ID_ENABLE`

This config enables adding of ID row intended for deduplication when inserting new records into bigquery.
Here is further documentation of bigquery streaming insert [deduplication](https://cloud.google.com/bigquery/streaming-data-into-bigquery).

- Example value: `false`
- Type: `required`
- Default value: `true`

#### `SINK_BIGQUERY_CREDENTIAL_PATH`

Full path of google cloud credentials file. Here is further documentation of google cloud authentication and [credentials](https://cloud.google.com/docs/authentication/getting-started).

- Example value: `/.secret/google-cloud-credentials.json`
- Type: `required`

#### `SINK_BIGQUERY_METADATA_NAMESPACE`

The name of column that will be added alongside of the existing bigquery column that generated from protobuf, that column contains struct of kafka metadata of the inserted record.
When this config is not configured the metadata column will not be added to the table.

- Example value: `kafka_metadata`
- Type: `optional`

#### `SINK_BIGQUERY_DATASET_LOCATION`

The geographic region name of location of bigquery dataset. Further documentation on bigquery dataset [location](https://cloud.google.com/bigquery/docs/locations#dataset_location).

- Example value: `us-central1`
- Type: `optional`
- Default value: `asia-southeast1`

#### `SINK_BIGQUERY_TABLE_PARTITION_EXPIRY_MS`

The duration of bigquery table partitioning expiration in milliseconds. Fill this config with `-1` will disable the table partition expiration. Further documentation on bigquery table partition [expiration](https://cloud.google.com/bigquery/docs/managing-partitioned-tables#partition-expiration).

- Example value: `2592000000`
- Type: `optional`
- Default value: `-1`

#### `SINK_BIGQUERY_CLIENT_READ_TIMEOUT_MS`

The duration of bigquery client http read timeout in milliseconds, 0 for an infinite timeout, a negative number for the default value (20000).

- Example value: `20000`
- Type: `optional`
- Default value: `-1`

#### `SINK_BIGQUERY_CLIENT_CONNECT_TIMEOUT_MS`

The duration of bigquery client http connection timeout in milliseconds, 0 for an infinite timeout, a negative number for the default value (20000).

- Example value: `20000`
- Type: `optional`
- Default value: `-1`

### Schema Registry

Stencil is dynamic schema registry for protobuf. Find more details about Stencil [here](https://github.com/odpf/stencil#stencil).

#### `SCHEMA_REGISTRY_STENCIL_ENABLE`

Enable/Disable using Stencil schema registry.

* Example value: `false`
* Type: `optional`
* Default value: `false`

#### `SCHEMA_REGISTRY_STENCIL_URLS`

Defines the stencil URL. Multiple URLs could be given in a comma-separated format.

* Example value: `http://localhost:8000/testproto.desc`
* Type: `required`

#### `SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE`

Enable/Disable the stencil refresh cache.

* Example value: `false`
* Type: `optional`
* Default value: `false`

#### `SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS`

Defines the stencil timeout in milliseconds.

* Example value: `60000`
* Type: `optional`
* Default value: `60000`

### Flink

#### `FLINK_PARALLELISM`

Defines the number of flink parallelism.

* Example value: `1`
* Type: `optional`
* Default value: `1`

#### `FLINK_WATERMARK_INTERVAL_MS`

Defines the flink watermark interval in milliseconds. Find more details on this config [here](https://ci.apache.org/projects/flink/flink-docs-master/docs/deployment/config/#pipeline-auto-watermark-interval).

* Example value: `10000`
* Type: `optional`
* Default value: `10000`

#### `FLINK_WATERMARK_DELAY_MS`

Defines the flink watermark delay in milliseconds.
* Example value: `10000`
* Type: `optional`
* Default value: `10000`

**Note:** For a stream configured with `PARQUET_SOURCE`, the watermark delay should be configured keeping in mind the 
partitioning that has been used in the root folder containing the files of `SOURCE_PARQUET_FILE_PATHS` configuration.
Currently, only [two types](../guides/create_dagger.md#parquet_source) of partitioning are supported: day and hour. Hence, 
* if partitioning is day wise, `FLINK_WATERMARK_DELAY_MS` should be set to 24 x 60 x 60 x 1000 milliseconds, that is, 
`86400000`.
* if partitioning is hour wise, `FLINK_WATERMARK_DELAY_MS` should be set to 60 x 60 x 1000 milliseconds, that is, 
`3600000`.

#### `FLINK_WATERMARK_PER_PARTITION_ENABLE`

Enable/Disable flink watermark per partition.

* Example value: `false`
* Type: `optional`
* Default value: `false`

#### `FLINK_CHECKPOINT_INTERVAL_MS`

Find more details about Flink checkpoint [here](https://ci.apache.org/projects/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/checkpointing/). 
Defines the flink checkpoint interval in milliseconds.

* Example value: `30000`
* Type: `optional`
* Default value: `30000`

#### `FLINK_CHECKPOINT_TIMEOUT_MS`

Defines the flink checkpoint timeout in milliseconds.

* Example value: `900000`
* Type: `optional`
* Default value: `900000`

#### `FLINK_CHECKPOINT_MIN_PAUSE_MS`

Defines the minimal pause between checkpointing attempts in milliseconds.

* Example value: `5000`
* Type: `optional`
* Default value: `5000`

#### `FLINK_CHECKPOINT_MAX_CONCURRENT`

Defines the maximum number of checkpoint attempts that may be in progress at the same time.

* Example value: `1`
* Type: `optional`
* Default value: `1`

#### `FLINK_RETENTION_MIN_IDLE_STATE_HOUR`

Find more details on `Flink Idle State Retention` [here](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/streaming/query_configuration.html#idle-state-retention-time).
Defines a minimum time interval for how long idle state in hours.

* Example value: `8`
* Type: `optional`
* Default value: `8`

#### `FLINK_RETENTION_MAX_IDLE_STATE_HOUR`

Defines a maximum time interval for how long idle state in hours.

* Example value: `9`
* Type: `optional`
* Default value: `9`

### Darts

Darts allows you to join streaming data from the reference data store. Darts provide a reference data store in the form of a list and <key, value> map and enable the refer-table in the form of UDF that can be used through the Flink SQL query.

Details of using Darts can be seen [here](../advance/DARTS.md).

#### `UDF_DART_GCS_PROJECT_ID`

Defines the GCS project id for Dart.

* Example value: `test-project`
* Type: `required`

#### `UDF_DART_GCS_BUCKET_ID`

Defines the GCS bucket id for Dart.

* Example value: `test-bucket`
* Type: `required`

### Longbow

Details of using Longbow can be seen [here](../advance/longbow.md).

#### `PROCESSOR_LONGBOW_ASYNC_TIMEOUT`

Defines the longbow async timeout.

* Example value: `15000L`
* Type: `optional`
* Default value: `15000L`

#### `PROCESSOR_LONGBOW_THREAD_CAPACITY`

Defines the longbow thread capacity.

* Example value: `30`
* Type: `optional`
* Default value: `30`

#### `PROCESSOR_LONGBOW_GCP_PROJECT_ID`

Defines the GCP project id for longbow.

* Example value: `test-longbow-project`
* Type: `required`

#### `PROCESSOR_LONGBOW_GCP_INSTANCE_ID`

Defines the GCP instance id for longbow.

* Example value: `test-longbow-instance`
* Type: `required`

#### `PROCESSOR_LONGBOW_GCP_TABLE_ID`

Defines the GCP Bigtable id for longbow.

* Example value: `test-longbow-table`
* Type: `required`

#### `PROCESSOR_LONGBOW_DOCUMENT_DURATION`

Defines the longbow document duration.

* Example value: `90d`
* Type: `optional`
* Default value: `90d`

will get `FLINK_JOB_ID` value for this config.

### PreProcessor

#### `PROCESSOR_PREPROCESSOR_ENABLE`

Enable/Disable using pre-processor.

* Example value: `false`
* Type: `optional`
* Default value: `false`

#### `PROCESSOR_PREPROCESSOR_CONFIG`

Details on this configuration can be seen on [advance documentation](../advance/pre_processor.md) of pre-processor.

### PostProcessor

#### `PROCESSOR_POSTPROCESSOR_ENABLE`

Enable/Disable using post-processor.

* Example value: `false`
* Type: `optional`
* Default value: `false`

#### `PROCESSOR_POSTPROCESSOR_CONFIG`

Details on this configuration can be seen on [advance documentation](../advance/post_processor.md) of post-processor.

### Telemetry

#### `METRIC_TELEMETRY_ENABLE`

Enable/Disable the flink telemetry for metric collection

* Example value: `true`
* Type: `optional`
* Default value: `true`

#### `METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS`

Shutdown period of metric telemetry in milliseconds.

* Example value: `10000`
* Type: `optional`
* Default value: `10000`

### Python Udfs

#### `PYTHON_UDF_ENABLE`

Enable/Disable using python udf.

* Example value: `10000`
* Type: `optional`
* Default value: `10000`

#### `PYTHON_UDF_CONFIG`

All the configuration need to use python udf.

These following variables need to be configured:

##### `PYTHON_FILES`

Defines the path of python udf files. Currently only support for `.py` and `.zip` data type. Comma (',') could be used as the separator to specify multiple files.

* Example value: `/path/to/files.zip`
* Type: `required`

##### `PYTHON_ARCHIVES`

Defines the path of files that used on the python udf. Only support for `.zip` data type. Comma (',') could be used as the separator to specify multiple archive files.
The archive files will be extracted to the working directory of python UDF worker. For each archive file, a target directory is specified. If the target directory name is specified, the archive file will be extracted to a directory with the specified name. Otherwise, the archive file will be extracted to a directory with the same name of the archive file. '#' could be used as the separator of the archive file path and the target directory name.

Example:
* PYTHON_ARCHIVES=/path/to/data.zip

  You should set the path name to `data.zip/data/sample.txt` on the udf to be able open the files.
  
* PYTHON_ARCHIVES=/path/to/data.zip#data
  
  You should set the path name to `data/sample.txt` on the udf to be able open the files.

* Example how to use this, can be found in this [udf](https://github.com/odpf/dagger/tree/main/dagger-py-functions/udfs/scalar/sample.py)
  
* Type: `optional`
* Default value: `(none)`

##### `PYTHON_REQUIREMENTS`

Defines the path of python dependency files. 

* Example value: `/path/to/requirements.txt`
* Type: `optional`
* Default value: `(none)`

##### `PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE`

The maximum number of elements to include in an arrow batch for python user-defined function execution.

* Example value: `10000`
* Type: `optional`
* Default value: `10000`

##### `PYTHON_FN_EXECUTION_BUNDLE_SIZE`

The maximum number of elements to include in a bundle for python user-defined function execution.

* Example value: `100000`
* Type: `optional`
* Default value: `100000`

##### `PYTHON_FN_EXECUTION_BUNDLE_TIME`

Sets the waiting timeout(in milliseconds) before processing a bundle for Python user-defined function execution. The timeout defines how long the elements of a bundle will be buffered before being processed.

* Example value: `1000`
* Type: `optional`
* Default value: `1000`

##### Sample Configuration
```
PYTHON_UDF_CONFIG = [
   {
      "PYTHON_FILES": "/path/to/files.py",
      "PYTHON_ARCHIVES": "/path/to/data.zip",
      "PYTHON_REQUIREMENTS": "/path/to/requirements.txt",
      "PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE": "10000",
      "PYTHON_FN_EXECUTION_BUNDLE_SIZE": "100000",
      "PYTHON_FN_EXECUTION_BUNDLE_TIME": "1000"
   }
]
```

Find more details on python udf config [here](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/python_config/#python-options).


