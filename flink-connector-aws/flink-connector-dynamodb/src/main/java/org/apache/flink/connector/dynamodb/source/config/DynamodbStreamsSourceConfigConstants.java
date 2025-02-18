/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.dynamodb.source.config;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Constants to be used with the DynamodbStreamsSource. */
@Experimental
public class DynamodbStreamsSourceConfigConstants {
    /** Marks the initial position to use when reading from the Dynamodb stream. */
    public enum InitialPosition {
        LATEST,
        TRIM_HORIZON
    }

    public static final ConfigOption<InitialPosition> STREAM_INITIAL_POSITION =
            ConfigOptions.key("flink.stream.initpos")
                    .enumType(InitialPosition.class)
                    .defaultValue(InitialPosition.LATEST)
                    .withDescription("The initial position to start reading Dynamodb streams.");

    public static final ConfigOption<Duration> SHARD_DISCOVERY_INTERVAL =
            ConfigOptions.key("flink.shard.discovery.intervalmillis")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription("The interval between each attempt to discover new shards.");

    public static final ConfigOption<Integer> DESCRIBE_STREAM_INCONSISTENCY_RESOLUTION_RETRY_COUNT =
            ConfigOptions.key("flink.describestream.inconsistencyresolution.retries")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "The number of times to retry build shard lineage if describestream returns inconsistent response");

    public static final ConfigOption<Integer> DYNAMODB_STREAMS_RETRY_COUNT =
            ConfigOptions.key("flink.dynamodbstreams.numretries")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "The number of times to retry DynamoDB Streams API call if it returns a retryable exception");

    public static final ConfigOption<Duration> DYNAMODB_STREAMS_EXPONENTIAL_BACKOFF_MIN_DELAY =
            ConfigOptions.key("flink.dynamodbstreams.backoff.mindelayduration")
                    .durationType()
                    .defaultValue(Duration.ofMillis(100))
                    .withDescription(
                            "The minimum delay for exponential backoff for describestream");

    public static final ConfigOption<Duration> DYNAMODB_STREAMS_EXPONENTIAL_BACKOFF_MAX_DELAY =
            ConfigOptions.key("flink.dynamodbstreams.backoff.maxdelay")
                    .durationType()
                    .defaultValue(Duration.ofMillis(1000))
                    .withDescription(
                            "The maximum delay for exponential backoff for describestream");

    public static final String BASE_DDB_STREAMS_USER_AGENT_PREFIX_FORMAT =
            "Apache Flink %s (%s) DynamoDb Streams Connector";

    public static final ConfigOption<Duration>
            DYNAMODB_STREAMS_GET_RECORDS_IDLE_TIME_BETWEEN_EMPTY_POLLS =
                    ConfigOptions.key(
                                    "flink.dynamodbstreams.getrecords.idle.time.between.empty.polls")
                            .durationType()
                            .defaultValue(Duration.ofMillis(1000))
                            .withDescription(
                                    "The idle time between empty polls for DynamoDB Streams GetRecords API");
    public static final ConfigOption<Duration>
            DYNAMODB_STREAMS_GET_RECORDS_IDLE_TIME_BETWEEN_NON_EMPTY_POLLS =
                    ConfigOptions.key(
                                    "flink.dynamodbstreams.getrecords.idle.time.between.nonempty.polls")
                            .durationType()
                            .defaultValue(Duration.ofMillis(250))
                            .withDescription(
                                    "The default idle time between non-empty polls for DynamoDB Streams GetRecords API");

    /** DynamoDb Streams identifier for user agent prefix. */
    public static final String DDB_STREAMS_CLIENT_USER_AGENT_PREFIX =
            "aws.dynamodbstreams.client.user-agent-prefix";
}
