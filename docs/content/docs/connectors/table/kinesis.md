---
title: Kinesis
weight: 5
type: docs
aliases:
- /dev/table/connectors/kinesis.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Amazon Kinesis Data Streams SQL Connector

{{< label "Scan Source: Unbounded" >}}
{{< label "Sink: Batch" >}}
{{< label "Sink: Streaming Append Mode" >}}

The Kinesis connector allows for reading data from and writing data into [Amazon Kinesis Data Streams (KDS)](https://aws.amazon.com/kinesis/data-streams/).

Dependencies
------------

{{< sql_connector_download_table "kinesis" >}}

The Kinesis connector is not part of the binary distribution.
See how to link with it for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).

### Versioning

There are two available Table API and SQL distributions for the Kinesis connector. 
This has resulted from an ongoing migration from the deprecated `SourceFunction` and `SinkFunction` interfaces to the new `Source` and `Sink` interfaces.

The Table API and SQL interfaces in Flink only allow one TableFactory for each connector identifier. 
Only one TableFactory with identifier `kinesis` can be included in your application's dependencies. 

The following table clarifies the underlying interface that is used depending on the distribution selected:

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 40%">Dependency</th>
      <th class="text-center" style="width: 10%">Connector Version</th>
      <th class="text-center" style="width: 25%">Source connector identifier (interface)</th>
      <th class="text-center" style="width: 25%">Sink connector identifier (interface)</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>flink-sql-connector-aws-kinesis-streams</code></td>
      <td><code>5.x</code> or later</td>
      <td><code>kinesis</code>(<code>Source</code>)</td>
      <td><code>kinesis</code>(<code>Sink</code>)</td>
    </tr>
    <tr>
      <td><code>flink-sql-connector-aws-kinesis-streams</code></td>
      <td><code>4.x</code> or earlier</td>
      <td>N/A (no source packaged)</td>
      <td><code>kinesis</code>(<code>Sink</code>)</td>
    </tr>
    <tr>
      <td><code>flink-sql-connector-kinesis</code></td>
      <td><code>5.x</code> or later</td>
      <td><code>kinesis</code>(<code>Source</code>), <code>kinesis-legacy</code>(<code>SourceFunction</code>)</td>
      <td><code>kinesis</code>(<code>Sink</code>)</td>
    </tr>
    <tr>
      <td><code>flink-sql-connector-kinesis</code></td>
      <td><code>4.x</code> or earlier</td>
      <td><code>kinesis</code>(<code>SourceFunction</code>)</td>
      <td><code>kinesis</code>(<code>Sink</code>)</td>
    </tr>
    </tbody>
</table>

{{< hint warning >}}
Only include one artifact, either `flink-sql-connector-aws-kinesis-streams` or `flink-sql-connector-kinesis`. Including both will result in clashing TableFactory names.
{{< /hint >}}

These docs are targeted for versions 5.x onwards. The main configuration section targets `kinesis` identifier.
For legacy configuration, please see [Configuration (`kinesis-legacy`)](#connector-options-kinesis-legacy)

### Migrating from v4.x to v5.x

There is no state compatibility between Table API and SQL API between 4.x and 5.x. 
This is due to the underlying implementation being changed.

Consider starting the job with v5.x `kinesis` table with `source.init.position` of `AT_TIMESTAMP` slightly before the time when the job with v4.x `kinesis` table was stopped.
Note that this may result in some re-processed some records.

How to create a Kinesis data stream table
-----------------------------------------

Follow the instructions from the [Amazon KDS Developer Guide](https://docs.aws.amazon.com/streams/latest/dev/introduction.html) to set up a Kinesis stream.
The following example shows how to create a table backed by a Kinesis data stream:

```sql
CREATE TABLE KinesisTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `category_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP(3)
)
PARTITIONED BY (user_id, item_id)
WITH (
  'connector' = 'kinesis',
  'stream.arn' = 'arn:aws:kinesis:us-east-1:012345678901:stream/my-stream-name',
  'aws.region' = 'us-east-1',
  'source.init.position' = 'LATEST',
  'format' = 'csv'
);
```

Available Metadata
------------------

{{< hint warning >}}
The `kinesis` table Source has a known bug that means `VIRTUAL` columns are not supported. 
Please use `kinesis-legacy` until [the fix](https://issues.apache.org/jira/browse/FLINK-36671) is completed. 
{{< /hint >}}

The following metadata can be exposed as read-only (`VIRTUAL`) columns in a table definition. This is only available in the `kinesis-legacy` connector only.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 25%">Key</th>
      <th class="text-center" style="width: 45%">Data Type</th>
      <th class="text-center" style="width: 35%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code><a href="https://docs.aws.amazon.com/kinesis/latest/APIReference/API_Record.html#Streams-Type-Record-ApproximateArrivalTimestamp">timestamp</a></code></td>
      <td><code>TIMESTAMP_LTZ(3) NOT NULL</code></td>
      <td>The approximate time when the record was inserted into the stream.</td>
    </tr>
    <tr>
      <td><code><a href="https://docs.aws.amazon.com/kinesis/latest/APIReference/API_Shard.html#Streams-Type-Shard-ShardId">shard-id</a></code></td>
      <td><code>VARCHAR(128) NOT NULL</code></td>
      <td>The unique identifier of the shard within the stream from which the record was read.</td>
    </tr>
    <tr>
      <td><code><a href="https://docs.aws.amazon.com/kinesis/latest/APIReference/API_Record.html#Streams-Type-Record-SequenceNumber">sequence-number</a></code></td>
      <td><code>VARCHAR(128) NOT NULL</code></td>
      <td>The unique identifier of the record within its shard.</td>
    </tr>
    </tbody>
</table>

The extended `CREATE TABLE` example demonstrates the syntax for exposing these metadata fields:

```sql
CREATE TABLE KinesisTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `category_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP(3),
  `arrival_time` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
  `shard_id` VARCHAR(128) NOT NULL METADATA FROM 'shard-id' VIRTUAL,
  `sequence_number` VARCHAR(128) NOT NULL METADATA FROM 'sequence-number' VIRTUAL
)
PARTITIONED BY (user_id, item_id)
WITH (
  'connector' = 'kinesis-legacy',
  'stream' = 'user_behavior',
  'aws.region' = 'us-east-2',
  'scan.stream.initpos' = 'LATEST',
  'format' = 'csv'
);
```


Connector Options
-----------------

<table class="table table-bordered">
    <thead>
    <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-center" style="width: 8%">Required</th>
        <th class="text-center" style="width: 8%">Forwarded</th>
        <th class="text-center" style="width: 7%">Default</th>
        <th class="text-center" style="width: 10%">Type</th>
        <th class="text-center" style="width: 42%">Description</th>
    </tr>
    <tr>
      <th colspan="6" class="text-left" style="width: 100%">Common Options</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>required</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what connector to use. For Kinesis use <code>'kinesis'</code> or <code>'kinesis-legacy'</code>. See <a href="#versioning">Versioning</a> for details.</td>
    </tr>
    <tr>
      <td><h5>stream.arn</h5></td>
      <td>required</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the Kinesis data stream backing this table.</td>
    </tr>
    <tr>
      <td><h5>format</h5></td>
      <td>required</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The format used to deserialize and serialize Kinesis data stream records. See <a href="#data-type-mapping">Data Type Mapping</a> for details.</td>
    </tr>
    <tr>
      <td><h5>aws.region</h5></td>
      <td>required</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The AWS region where the stream is defined.</td>
    </tr>
    <tr>
      <td><h5>aws.endpoint</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The AWS endpoint for Kinesis (derived from the AWS region setting if not set).</td>
    </tr>
    <tr>
      <td><h5>aws.trust.all.certificates</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>If true accepts all SSL certificates. This is not recommended for production environments, but should only be used for testing purposes.</td>
    </tr>
    </tbody>
    <thead>
    <tr>
      <th colspan="6" class="text-left" style="width: 100%">Authentication Options</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>aws.credentials.provider</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">AUTO</td>
      <td>String</td>
      <td>A credentials provider to use when authenticating against the Kinesis endpoint. See <a href="#authentication">Authentication</a> for details.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.basic.accesskeyid</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The AWS access key ID to use when setting credentials provider type to BASIC.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.basic.secretkey</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The AWS secret key to use when setting credentials provider type to BASIC.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.profile.path</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>Optional configuration for profile path if credential provider type is set to be PROFILE.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.profile.name</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>Optional configuration for profile name if credential provider type is set to be PROFILE.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.role.arn</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The role ARN to use when credential provider type is set to ASSUME_ROLE or WEB_IDENTITY_TOKEN.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.role.sessionName</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The role session name to use when credential provider type is set to ASSUME_ROLE or WEB_IDENTITY_TOKEN.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.role.externalId</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The external ID to use when credential provider type is set to ASSUME_ROLE.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.role.stsEndpoint</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The AWS endpoint for STS (derived from the AWS region setting if not set) to use when credential provider type is set to ASSUME_ROLE.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.role.provider</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The credentials provider that provides credentials for assuming the role when credential provider type is set to ASSUME_ROLE. Roles can be nested, so this value can again be set to ASSUME_ROLE</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.webIdentityToken.file</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The absolute path to the web identity token file that should be used if provider type is set to WEB_IDENTITY_TOKEN.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.custom.class</h5></td>
	  <td>required only if credential provider is set to CUSTOM</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The full path (in Java package notation) to the user provided
      class to use if credential provider type is set to be CUSTOM e.g. org.user_company.auth.CustomAwsCredentialsProvider.</td>
    </tr>
    </tbody>
    <thead>
    <tr>
      <th colspan="6" class="text-left" style="width: 100%">Source Options</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>source.init.position</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">LATEST</td>
      <td>String</td>
      <td>Initial position to be used when reading from the table.</td>
    </tr>
    <tr>
      <td><h5>source.init.timestamp</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The initial timestamp to start reading Kinesis stream from (when <code>source.init.position</code> is AT_TIMESTAMP).</td>
    </tr>
    <tr>
      <td><h5>source.init.timestamp.format</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">yyyy-MM-dd'T'HH:mm:ss.SSSXXX</td>
      <td>String</td>
      <td>The date format of initial timestamp to start reading Kinesis stream from (when <code>source.init.position</code> is AT_TIMESTAMP).</td>
    </tr>
    <tr>
      <td><h5>source.shard.discovery.interval</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">10 s</td>
      <td>Duration</td>
      <td>The interval between each attempt to discover new shards.</td>
    </tr>
    <tr>
      <td><h5>source.reader.type</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">POLLING</td>
      <td>String</td>
      <td>The <code>ReaderType</code> to use for sources (<code>POLLING|EFO</code>).</td>
    </tr>
    <tr>
      <td><h5>source.shard.get-records.max-record-count</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">10000</td>
      <td>Integer</td>
      <td>Only applicable to POLLING <code>ReaderType</code>. The maximum number of records to try to get each time we fetch records from a AWS Kinesis shard.</td>
    </tr>
    <tr>
      <td><h5>source.efo.consumer.name</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Only applicable to EFO <code>ReaderType</code>. The name of the EFO consumer to register with KDS.</td>
    </tr>
    <tr>
      <td><h5>source.efo.lifecycle</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">JOB_MANAGED</td>
      <td>String</td>
      <td>Only applicable to EFO <code>ReaderType</code>. Determine if the EFO consumer is managed by the Flink job <code>JOB_MANAGED|SELF_MANAGED</code>.</td>
    </tr>
    <tr>
      <td><h5>source.efo.subscription.timeout</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">60 s</td>
      <td>Duration</td>
      <td>Only applicable to EFO <code>ReaderType</code>. Timeout for EFO Consumer subscription.</td>
    </tr>
    <tr>
      <td><h5>source.efo.deregister.timeout</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">10 s</td>
      <td>Duration</td>
      <td>Only applicable to EFO <code>ReaderType</code>. Timeout for consumer deregistration. When timeout is reached, code will continue as per normal.</td>
    </tr>
    <tr>
      <td><h5>source.efo.describe.retry-strategy.attempts.max</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">100</td>
      <td>Integer</td>
      <td>Only applicable to EFO <code>ReaderType</code>. Maximum number of attempts for the exponential backoff retry strategy when calling <code>DescribeStreamConsumer</code>.</td>
    </tr>
    <tr>
      <td><h5>source.efo.describe.retry-strategy.delay.min</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">2 s</td>
      <td>Duration</td>
      <td>Only applicable to EFO <code>ReaderType</code>. Base delay for the exponential backoff retry strategy when calling <code>DescribeStreamConsumer</code>.</td>
    </tr>
    <tr>
      <td><h5>source.efo.describe.retry-strategy.delay.max</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">60 s</td>
      <td>Duration</td>
      <td>Only applicable to EFO <code>ReaderType</code>. Max delay for the exponential backoff retry strategy when calling <code>DescribeStreamConsumer</code>.</td>
    </tr>
    </tbody>
    <thead>
    <tr>
      <th colspan="6" class="text-left" style="width: 100%">Sink Options</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>sink.partitioner</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">random or row-based</td>
      <td>String</td>
      <td>Optional output partitioning from Flink's partitions into Kinesis shards. See <a href="#sink-partitioning">Sink Partitioning</a> for details.</td>
    </tr>
    <tr>
      <td><h5>sink.partitioner-field-delimiter</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">|</td>
      <td>String</td>
      <td>Optional field delimiter for a fields-based partitioner derived from a PARTITION BY clause. See <a href="#sink-partitioning">Sink Partitioning</a> for details.</td>
    </tr>
    <tr>
      <td><h5>sink.producer.*</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td></td>
      <td>
        Deprecated options previously used by the legacy connector.
        Options with equivalant alternatives in <code>KinesisStreamsSink</code> are matched 
        to their respective properties. Unsupported options are logged out to user as warnings.
      </td>
    </tr>
    <tr>
      <td><h5>sink.http-client.max-concurrency</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">10000</td>
      <td>Integer</td>
      <td>
      Maximum number of allowed concurrent requests by <code>KinesisAsyncClient</code>.
      </td>
    </tr>
    <tr>
      <td><h5>sink.http-client.read-timeout</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">360000</td>
      <td>Integer</td>
      <td>
        Maximum amount of time in ms for requests to be sent by <code>KinesisAsyncClient</code>.
      </td>
    </tr>
    <tr>
      <td><h5>sink.http-client.protocol.version</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">HTTP2</td>
      <td>String</td>
      <td>Http version used by Kinesis Client.</td>
    </tr>
    <tr>
      <td><h5>sink.batch.max-size</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">500</td>
      <td>Integer</td>
      <td>Maximum batch size of elements to be passed to <code>KinesisAsyncClient</code> to be written downstream.</td>
    </tr>
    <tr>
      <td><h5>sink.requests.max-inflight</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">16</td>
      <td>Integer</td>
      <td>Request threshold for uncompleted requests by <code>KinesisAsyncClient</code>before blocking new write requests and applying backpressure.</td>
    </tr>
    <tr>
      <td><h5>sink.requests.max-buffered</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">10000</td>
      <td>String</td>
      <td>Request buffer threshold for buffered requests by <code>KinesisAsyncClient</code> before blocking new write requests and applying backpressure.</td>
    </tr>
    <tr>
      <td><h5>sink.flush-buffer.size</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">5242880</td>
      <td>Long</td>
      <td>Threshold value in bytes for writer buffer in <code>KinesisAsyncClient</code> before flushing.</td>
    </tr>
    <tr>
      <td><h5>sink.flush-buffer.timeout</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">5000</td>
      <td>Long</td>
      <td>Threshold time in milliseconds for an element to be in a buffer of<code>KinesisAsyncClient</code> before flushing.</td>
    </tr>
    <tr>
      <td><h5>sink.fail-on-error</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Flag used for retrying failed requests. If set any request failure will not be retried and will fail the job.</td>
    </tr>
    </tbody>
</table>

Features
--------

{{< hint info >}}
Refer to the [Kinesis Datastream API]({{< ref "docs/connectors/datastream/kinesis" >}}) documentation for more detailed description of features.
{{< /hint >}}

### Sink Partitioning

Kinesis data streams consist of one or more shards, and the `sink.partitioner` option allows you to control how records written into a multi-shard Kinesis-backed table will be partitioned between its shards.
Valid values are:

* `fixed`: Kinesis `PartitionKey` values derived from the Flink subtask index, so each Flink partition ends up in at most one Kinesis partition (assuming that no re-sharding takes place at runtime).
* `random`: Kinesis `PartitionKey` values are assigned randomly. This is the default value for tables not defined with a `PARTITION BY` clause.
* Custom `FixedKinesisPartitioner` subclass: e.g. `'org.mycompany.MyPartitioner'`.

{{< hint info >}}
Records written into tables defining a `PARTITION BY` clause will always be partitioned based on a concatenated projection of the `PARTITION BY` fields.
In this case, the `sink.partitioner` field cannot be used to modify this behavior (attempting to do this results in a configuration error).
You can, however, use the `sink.partitioner-field-delimiter` option to set the delimiter of field values in the concatenated [PartitionKey](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html#Streams-PutRecord-request-PartitionKey) string (an empty string is also a valid delimiter).
{{< /hint >}}

# Data Type Mapping

Kinesis stores records as Base64-encoded binary data objects, so it doesn't have a notion of internal record structure.
Instead, Kinesis records are deserialized and serialized by formats, e.g. 'avro', 'csv', or 'json'.
To determine the data type of the messages in your Kinesis-backed tables, pick a suitable Flink format with the `format` keyword.
Please refer to the [Formats]({{< ref "docs/connectors/table/formats/overview" >}}) pages for more details.

Connector Options (`kinesis-legacy`)
-----------------

<table class="table table-bordered">
    <thead>
    <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-center" style="width: 8%">Required</th>
        <th class="text-center" style="width: 8%">Forwarded</th>
        <th class="text-center" style="width: 7%">Default</th>
        <th class="text-center" style="width: 10%">Type</th>
        <th class="text-center" style="width: 42%">Description</th>
    </tr>
    <tr>
      <th colspan="6" class="text-left" style="width: 100%">Common Options</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>required</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what connector to use. For Kinesis use <code>'kinesis'</code>.</td>
    </tr>
    <tr>
      <td><h5>stream</h5></td>
      <td>required</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the Kinesis data stream backing this table.</td>
    </tr>
    <tr>
      <td><h5>format</h5></td>
      <td>required</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The format used to deserialize and serialize Kinesis data stream records. See <a href="#data-type-mapping">Data Type Mapping</a> for details.</td>
    </tr>
    <tr>
      <td><h5>aws.region</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The AWS region where the stream is defined. Either this or <code>aws.endpoint</code> are required.</td>
    </tr>
    <tr>
      <td><h5>aws.endpoint</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The AWS endpoint for Kinesis (derived from the AWS region setting if not set). Either this or <code>aws.region</code> are required.</td>
    </tr>
    <tr>
      <td><h5>aws.trust.all.certificates</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>If true accepts all SSL certificates.</td>
    </tr>
    </tbody>
    <thead>
    <tr>
      <th colspan="6" class="text-left" style="width: 100%">Authentication Options</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>aws.credentials.provider</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">AUTO</td>
      <td>String</td>
      <td>A credentials provider to use when authenticating against the Kinesis endpoint. See <a href="#authentication">Authentication</a> for details.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.basic.accesskeyid</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The AWS access key ID to use when setting credentials provider type to BASIC.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.basic.secretkey</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The AWS secret key to use when setting credentials provider type to BASIC.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.profile.path</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>Optional configuration for profile path if credential provider type is set to be PROFILE.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.profile.name</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>Optional configuration for profile name if credential provider type is set to be PROFILE.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.role.arn</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The role ARN to use when credential provider type is set to ASSUME_ROLE or WEB_IDENTITY_TOKEN.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.role.sessionName</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The role session name to use when credential provider type is set to ASSUME_ROLE or WEB_IDENTITY_TOKEN.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.role.externalId</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The external ID to use when credential provider type is set to ASSUME_ROLE.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.role.stsEndpoint</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The AWS endpoint for STS (derived from the AWS region setting if not set) to use when credential provider type is set to ASSUME_ROLE.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.role.provider</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The credentials provider that provides credentials for assuming the role when credential provider type is set to ASSUME_ROLE. Roles can be nested, so this value can again be set to ASSUME_ROLE</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.webIdentityToken.file</h5></td>
	  <td>optional</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The absolute path to the web identity token file that should be used if provider type is set to WEB_IDENTITY_TOKEN.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.custom.class</h5></td>
	  <td>required only if credential provider is set to CUSTOM</td>
      <td>no</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The full path (in Java package notation) to the user provided
      class to use if credential provider type is set to be CUSTOM e.g. org.user_company.auth.CustomAwsCredentialsProvider.</td>
    </tr>
    </tbody>
    <thead>
    <tr>
      <th colspan="6" class="text-left" style="width: 100%">Source Options</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>scan.stream.initpos</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">LATEST</td>
      <td>String</td>
      <td>Initial position to be used when reading from the table. See <a href="#start-reading-position">Start Reading Position</a> for details.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.initpos-timestamp</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The initial timestamp to start reading Kinesis stream from (when <code>scan.stream.initpos</code> is AT_TIMESTAMP). See <a href="#start-reading-position">Start Reading Position</a> for details.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.initpos-timestamp-format</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">yyyy-MM-dd'T'HH:mm:ss.SSSXXX</td>
      <td>String</td>
      <td>The date format of initial timestamp to start reading Kinesis stream from (when <code>scan.stream.initpos</code> is AT_TIMESTAMP). See <a href="#start-reading-position">Start Reading Position</a> for details.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.recordpublisher</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">POLLING</td>
      <td>String</td>
      <td>The <code>RecordPublisher</code> type to use for sources.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.efo.consumername</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of the EFO consumer to register with KDS.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.efo.registration</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">LAZY</td>
      <td>String</td>
      <td>Determine how and when consumer de-/registration is performed (LAZY|EAGER|NONE).</td>
    </tr>
    <tr>
      <td><h5>scan.stream.efo.consumerarn</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The prefix of consumer ARN for a given stream.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.efo.http-client.max-concurrency</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">10000</td>
      <td>Integer</td>
      <td>Maximum number of allowed concurrent requests for the EFO client.</td>
    </tr>
    <tr>
      <td><h5>scan.shard-assigner</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">default</td>
      <td>String</td>
      <td>The shard assigner used to map shards to Flink subtasks (default|uniform). You can also supply your own shard assigner via the Java Service Provider Interfaces (SPI).</td></td>
    </tr>
    <tr>
      <td><h5>scan.stream.describe.maxretries</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">50</td>
      <td>Integer</td>
      <td>The maximum number of <code>describeStream</code> attempts if we get a recoverable exception.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.describe.backoff.base</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">2000</td>
      <td>Long</td>
      <td>The base backoff time (in milliseconds) between each <code>describeStream</code> attempt (for consuming from DynamoDB streams).</td>
    </tr>
    <tr>
      <td><h5>scan.stream.describe.backoff.max</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">5000</td>
      <td>Long</td>
      <td>The maximum backoff time (in milliseconds)  between each <code>describeStream</code> attempt (for consuming from DynamoDB streams).</td>
    </tr>
    <tr>
      <td><h5>scan.stream.describe.backoff.expconst</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">1.5</td>
      <td>Double</td>
      <td>The power constant for exponential backoff between each <code>describeStream</code> attempt (for consuming from DynamoDB streams).</td>
    </tr>
    <tr>
      <td><h5>scan.list.shards.maxretries</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">10</td>
      <td>Integer</td>
      <td>The maximum number of <code>listShards</code> attempts if we get a recoverable exception.</td>
    </tr>
    <tr>
      <td><h5>scan.list.shards.backoff.base</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Long</td>
      <td>The base backoff time (in milliseconds) between each <code>listShards</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.list.shards.backoff.max</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">5000</td>
      <td>Long</td>
      <td>The maximum backoff time (in milliseconds) between each <code>listShards</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.list.shards.backoff.expconst</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">1.5</td>
      <td>Double</td>
      <td>The power constant for exponential backoff between each <code>listShards</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.describestreamconsumer.maxretries</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">50</td>
      <td>Integer</td>
      <td>The maximum number of <code>describeStreamConsumer</code> attempts if we get a recoverable exception.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.describestreamconsumer.backoff.base</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">2000</td>
      <td>Long</td>
      <td>The base backoff time (in milliseconds) between each <code>describeStreamConsumer</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.describestreamconsumer.backoff.max</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">5000</td>
      <td>Long</td>
      <td>The maximum backoff time (in milliseconds) between each <code>describeStreamConsumer</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.describestreamconsumer.backoff.expconst</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">1.5</td>
      <td>Double</td>
      <td>The power constant for exponential backoff between each <code>describeStreamConsumer</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.registerstreamconsumer.maxretries</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">10</td>
      <td>Integer</td>
      <td>The maximum number of <code>registerStream</code> attempts if we get a recoverable exception.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.registerstreamconsumer.timeout</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">60</td>
      <td>Integer</td>
      <td>The maximum time in seconds to wait for a stream consumer to become active before giving up.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.registerstreamconsumer.backoff.base</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">500</td>
      <td>Long</td>
      <td>The base backoff time (in milliseconds) between each <code>registerStream</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.registerstreamconsumer.backoff.max</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">2000</td>
      <td>Long</td>
      <td>The maximum backoff time (in milliseconds) between each <code>registerStream</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.registerstreamconsumer.backoff.expconst</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">1.5</td>
      <td>Double</td>
      <td>The power constant for exponential backoff between each <code>registerStream</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.deregisterstreamconsumer.maxretries</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">10</td>
      <td>Integer</td>
      <td>The maximum number of <code>deregisterStream</code> attempts if we get a recoverable exception.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.deregisterstreamconsumer.timeout</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">60</td>
      <td>Integer</td>
      <td>The maximum time in seconds to wait for a stream consumer to deregister before giving up.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.deregisterstreamconsumer.backoff.base</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">500</td>
      <td>Long</td>
      <td>The base backoff time (in milliseconds) between each <code>deregisterStream</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.deregisterstreamconsumer.backoff.max</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">2000</td>
      <td>Long</td>
      <td>The maximum backoff time (in milliseconds) between each <code>deregisterStream</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.deregisterstreamconsumer.backoff.expconst</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">1.5</td>
      <td>Double</td>
      <td>The power constant for exponential backoff between each <code>deregisterStream</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.subscribetoshard.maxretries</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">10</td>
      <td>Integer</td>
      <td>The maximum number of <code>subscribeToShard</code> attempts if we get a recoverable exception.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.subscribetoshard.backoff.base</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Long</td>
      <td>The base backoff time (in milliseconds) between each <code>subscribeToShard</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.subscribetoshard.backoff.max</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">2000</td>
      <td>Long</td>
      <td>The maximum backoff time (in milliseconds) between each <code>subscribeToShard</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.subscribetoshard.backoff.expconst</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">1.5</td>
      <td>Double</td>
      <td>The power constant for exponential backoff between each <code>subscribeToShard</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getrecords.maxrecordcount</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">10000</td>
      <td>Integer</td>
      <td>The maximum number of records to try to get each time we fetch records from a AWS Kinesis shard.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getrecords.maxretries</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>The maximum number of <code>getRecords</code> attempts if we get a recoverable exception.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getrecords.backoff.base</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">300</td>
      <td>Long</td>
      <td>The base backoff time (in milliseconds) between <code>getRecords</code> attempts if we get a ProvisionedThroughputExceededException.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getrecords.backoff.max</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Long</td>
      <td>The maximum backoff time (in milliseconds) between <code>getRecords</code> attempts if we get a ProvisionedThroughputExceededException.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getrecords.backoff.expconst</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">1.5</td>
      <td>Double</td>
      <td>The power constant for exponential backoff between each <code>getRecords</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getrecords.intervalmillis</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">200</td>
      <td>Long</td>
      <td>The interval (in milliseconds) between each <code>getRecords</code> request to a AWS Kinesis shard in milliseconds.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getiterator.maxretries</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>The maximum number of <code>getShardIterator</code> attempts if we get ProvisionedThroughputExceededException.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getiterator.backoff.base</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">300</td>
      <td>Long</td>
      <td>The base backoff time (in milliseconds) between <code>getShardIterator</code> attempts if we get a ProvisionedThroughputExceededException.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getiterator.backoff.max</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Long</td>
      <td>The maximum backoff time (in milliseconds) between <code>getShardIterator</code> attempts if we get a ProvisionedThroughputExceededException.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getiterator.backoff.expconst</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">1.5</td>
      <td>Double</td>
      <td>The power constant for exponential backoff between each <code>getShardIterator</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.discovery.intervalmillis</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">10000</td>
      <td>Integer</td>
      <td>The interval between each attempt to discover new shards.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.adaptivereads</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>The config to turn on adaptive reads from a shard. See the <code>AdaptivePollingRecordPublisher</code> documentation for details.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.idle.interval</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">-1</td>
      <td>Long</td>
      <td>The interval (in milliseconds) after which to consider a shard idle for purposes of watermark generation. A positive value will allow the watermark to progress even when some shards don't receive new records.</td>
    </tr>
   <tr>
      <td><h5>shard.consumer.error.recoverable[0].exception</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>User-specified Exception to retry indefinitely. Example value: `java.net.UnknownHostException`. This configuration is a zero-based array. As such, the specified exceptions must start with index 0. Specified exceptions must be valid Throwables in classpath, or connector will fail to initialize and fail fast.</td>
    </tr> 
    <tr>
      <td><h5>scan.watermark.sync.interval</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">30000</td>
      <td>Long</td>
      <td>The interval (in milliseconds) for periodically synchronizing the shared watermark state.</td>
    </tr>
    <tr>
      <td><h5>scan.watermark.lookahead.millis</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">0</td>
      <td>Long</td>
      <td>The maximum delta (in milliseconds) allowed for the reader to advance ahead of the shared global watermark.</td>
    </tr>
    <tr>
      <td><h5>scan.watermark.sync.queue.capacity</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">100</td>
      <td>Integer</td>
      <td>The maximum number of records that will be buffered before suspending consumption of a shard.</td>
    </tr>
    </tbody>
    <thead>
    <tr>
      <th colspan="6" class="text-left" style="width: 100%">Sink Options</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>sink.partitioner</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">random or row-based</td>
      <td>String</td>
      <td>Optional output partitioning from Flink's partitions into Kinesis shards. See <a href="#sink-partitioning">Sink Partitioning</a> for details.</td>
    </tr>
    <tr>
      <td><h5>sink.partitioner-field-delimiter</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">|</td>
      <td>String</td>
      <td>Optional field delimiter for a fields-based partitioner derived from a PARTITION BY clause. See <a href="#sink-partitioning">Sink Partitioning</a> for details.</td>
    </tr>
    <tr>
      <td><h5>sink.producer.*</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td></td>
      <td>
        Deprecated options previously used by the legacy connector.
        Options with equivalant alternatives in <code>KinesisStreamsSink</code> are matched 
        to their respective properties. Unsupported options are logged out to user as warnings.
      </td>
    </tr>
    <tr>
      <td><h5>sink.http-client.max-concurrency</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">10000</td>
      <td>Integer</td>
      <td>
      Maximum number of allowed concurrent requests by <code>KinesisAsyncClient</code>.
      </td>
    </tr>
    <tr>
      <td><h5>sink.http-client.read-timeout</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">360000</td>
      <td>Integer</td>
      <td>
        Maximum amount of time in ms for requests to be sent by <code>KinesisAsyncClient</code>.
      </td>
    </tr>
    <tr>
      <td><h5>sink.http-client.protocol.version</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">HTTP2</td>
      <td>String</td>
      <td>Http version used by Kinesis Client.</td>
    </tr>
    <tr>
      <td><h5>sink.batch.max-size</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">500</td>
      <td>Integer</td>
      <td>Maximum batch size of elements to be passed to <code>KinesisAsyncClient</code> to be written downstream.</td>
    </tr>
    <tr>
      <td><h5>sink.requests.max-inflight</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">16</td>
      <td>Integer</td>
      <td>Request threshold for uncompleted requests by <code>KinesisAsyncClient</code>before blocking new write requests and applying backpressure.</td>
    </tr>
    <tr>
      <td><h5>sink.requests.max-buffered</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">10000</td>
      <td>String</td>
      <td>Request buffer threshold for buffered requests by <code>KinesisAsyncClient</code> before blocking new write requests and applying backpressure.</td>
    </tr>
    <tr>
      <td><h5>sink.flush-buffer.size</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">5242880</td>
      <td>Long</td>
      <td>Threshold value in bytes for writer buffer in <code>KinesisAsyncClient</code> before flushing.</td>
    </tr>
    <tr>
      <td><h5>sink.flush-buffer.timeout</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">5000</td>
      <td>Long</td>
      <td>Threshold time in milliseconds for an element to be in a buffer of<code>KinesisAsyncClient</code> before flushing.</td>
    </tr>
    <tr>
      <td><h5>sink.fail-on-error</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Flag used for retrying failed requests. If set any request failure will not be retried and will fail the job.</td>
    </tr>
    </tbody>
</table>

{{< top >}}
