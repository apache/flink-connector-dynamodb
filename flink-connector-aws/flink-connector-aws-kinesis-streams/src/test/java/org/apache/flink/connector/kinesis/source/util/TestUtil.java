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

package org.apache.flink.connector.kinesis.source.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.connector.kinesis.source.metrics.MetricConstants;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.testutils.MetricListener;

import com.amazonaws.kinesis.agg.AggRecord;
import com.amazonaws.kinesis.agg.RecordAggregator;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.HashKeyRange;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Utilities class for testing Kinesis Source. */
public class TestUtil {

    public static final String STREAM_ARN =
            "arn:aws:kinesis:us-east-1:123456789012:stream/keenesesStream";
    public static final String SHARD_ID = "shardId-000000000002";
    public static final String CONSUMER_ARN =
            "arn:aws:kinesis:us-east-1:123456789012:stream/stream-name/consumer/consumer-name:1722967044";
    public static final SimpleStringSchema STRING_SCHEMA = new SimpleStringSchema();
    public static final long MILLIS_BEHIND_LATEST_TEST_VALUE = 100L;
    public static final String STARTING_HASH_KEY_TEST_VALUE =
            "42535295865117307932921825928971026432";
    public static final String ENDING_HASH_KEY_TEST_VALUE =
            "85070591730234615865843651857942052863";

    public static String generateShardId(int shardId) {
        return String.format("shardId-%012d", shardId);
    }

    public static Shard[] generateShardsWithEqualHashKeyRange(int numberOfShards) {
        BigInteger two = BigInteger.valueOf(2);
        BigInteger numShards = BigInteger.valueOf(numberOfShards);
        BigInteger maxHashKey = two.pow(128).subtract(BigInteger.ONE);
        BigInteger[] hashKeyRangeBoundaries = new BigInteger[numberOfShards + 1];
        for (int i = 0; i <= numberOfShards; i++) {
            hashKeyRangeBoundaries[i] =
                    maxHashKey.multiply(BigInteger.valueOf(i)).divide(numShards);
        }
        return IntStream.range(0, numberOfShards)
                .mapToObj(
                        i ->
                                Shard.builder()
                                        .shardId(generateShardId(i))
                                        .hashKeyRange(
                                                HashKeyRange.builder()
                                                        .startingHashKey(
                                                                hashKeyRangeBoundaries[i]
                                                                        .toString())
                                                        .endingHashKey(
                                                                hashKeyRangeBoundaries[i + 1]
                                                                        .toString())
                                                        .build())
                                        .build())
                .toArray(Shard[]::new);
    }

    public static KinesisShardSplitState getTestSplitState() {
        return new KinesisShardSplitState(getTestSplit());
    }

    public static KinesisShardSplitState getTestSplitState(KinesisShardSplit testSplit) {
        return new KinesisShardSplitState(testSplit);
    }

    public static KinesisShardSplit getTestSplit() {
        return getTestSplit(SHARD_ID);
    }

    public static KinesisShardSplit getTestSplit(String shardId) {
        return getTestSplit(STREAM_ARN, shardId);
    }

    public static KinesisShardSplit getTestSplit(String shardId, Set<String> parentShards) {
        return getTestSplit(STREAM_ARN, shardId, parentShards);
    }

    public static KinesisShardSplit getTestSplit(String streamArn, String shardId) {
        return new KinesisShardSplit(
                streamArn,
                shardId,
                StartingPosition.fromStart(),
                Collections.emptySet(),
                STARTING_HASH_KEY_TEST_VALUE,
                ENDING_HASH_KEY_TEST_VALUE);
    }

    public static KinesisShardSplit getTestSplit(
            String streamArn, String shardId, Set<String> parentShards) {
        return new KinesisShardSplit(
                streamArn,
                shardId,
                StartingPosition.fromStart(),
                parentShards,
                STARTING_HASH_KEY_TEST_VALUE,
                ENDING_HASH_KEY_TEST_VALUE);
    }

    public static KinesisShardSplit getTestSplit(StartingPosition startingPosition) {
        return new KinesisShardSplit(
                STREAM_ARN,
                SHARD_ID,
                startingPosition,
                Collections.emptySet(),
                STARTING_HASH_KEY_TEST_VALUE,
                ENDING_HASH_KEY_TEST_VALUE);
    }

    public static KinesisShardSplit getTestSplit(
            BigInteger startingHashKey, BigInteger endingHashKey) {
        return new KinesisShardSplit(
                STREAM_ARN,
                SHARD_ID,
                StartingPosition.fromStart(),
                Collections.emptySet(),
                startingHashKey.toString(),
                endingHashKey.toString());
    }

    public static ReaderInfo getTestReaderInfo(final int subtaskId) {
        return new ReaderInfo(subtaskId, "some-location");
    }

    public static Record getTestRecord(String data) {
        return Record.builder()
                .data(SdkBytes.fromByteArray(STRING_SCHEMA.serialize(data)))
                .approximateArrivalTimestamp(Instant.now())
                .build();
    }

    public static Record convertToRecord(KinesisClientRecord record) {
        return Record.builder()
                .data(SdkBytes.fromByteBuffer(record.data()))
                .approximateArrivalTimestamp(record.approximateArrivalTimestamp())
                .build();
    }

    public static List<KinesisClientRecord> convertToKinesisClientRecord(List<Record> records) {
        return records.stream().map(KinesisClientRecord::fromRecord).collect(Collectors.toList());
    }

    public static Record createAggregatedRecord(List<Record> records) {
        KinesisClientRecord aggregatedRecord = createKinesisAggregatedRecord(records);
        return convertToRecord(aggregatedRecord);
    }

    public static KinesisClientRecord createKinesisAggregatedRecord(List<Record> records) {
        RecordAggregator recordAggregator = new RecordAggregator();

        for (Record record : records) {
            try {
                recordAggregator.addUserRecord("key", record.data().asByteArray());
            } catch (Exception e) {
                throw new RuntimeException("Failed to add record to aggregator", e);
            }
        }

        AggRecord aggRecord = recordAggregator.clearAndGet();

        return KinesisClientRecord.builder()
                .data(ByteBuffer.wrap(aggRecord.toRecordBytes()))
                .partitionKey(aggRecord.getPartitionKey())
                .explicitHashKey(aggRecord.getExplicitHashKey())
                .approximateArrivalTimestamp(Instant.now())
                .build();
    }

    public static void assertMillisBehindLatest(
            KinesisShardSplit split, long expectedValue, MetricListener metricListener) {
        Arn kinesisArn = Arn.fromString(split.getStreamArn());

        final Optional<Gauge<Long>> millisBehindLatestGauge =
                metricListener.getGauge(
                        MetricConstants.KINESIS_STREAM_SOURCE_METRIC_GROUP,
                        MetricConstants.ACCOUNT_ID_METRIC_GROUP,
                        kinesisArn.accountId().get(),
                        MetricConstants.REGION_METRIC_GROUP,
                        kinesisArn.region().get(),
                        MetricConstants.STREAM_METRIC_GROUP,
                        kinesisArn.resource().resource(),
                        MetricConstants.SHARD_METRIC_GROUP,
                        split.getShardId(),
                        MetricConstants.MILLIS_BEHIND_LATEST);

        assertThat(millisBehindLatestGauge).isPresent();
        assertThat((long) millisBehindLatestGauge.get().getValue()).isEqualTo(expectedValue);
    }
}
