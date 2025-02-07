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

package org.apache.flink.connector.kinesis.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.kinesis.source.serialization.KinesisDeserializationSchema;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplitState;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class KinesisStreamsRecordEmitterTest {

    private static final SimpleStringSchema STRING_SCHEMA = new SimpleStringSchema();

    @Test
    void testEmitRecord() throws Exception {
        final Instant startTime = Instant.now();
        List<KinesisClientRecord> inputRecords =
                Stream.of(
                                KinesisClientRecord.builder()
                                        .data(ByteBuffer.wrap(STRING_SCHEMA.serialize("data-1")))
                                        .approximateArrivalTimestamp(startTime)
                                        .build(),
                                KinesisClientRecord.builder()
                                        .data(ByteBuffer.wrap(STRING_SCHEMA.serialize("data-2")))
                                        .approximateArrivalTimestamp(startTime.plusSeconds(10))
                                        .build(),
                                KinesisClientRecord.builder()
                                        .data(ByteBuffer.wrap(STRING_SCHEMA.serialize("data-3")))
                                        .approximateArrivalTimestamp(startTime.plusSeconds(20))
                                        .sequenceNumber("some-sequence-number")
                                        .build())
                        .collect(Collectors.toList());
        final StartingPosition expectedStartingPosition =
                StartingPosition.continueFromSequenceNumber("some-sequence-number");
        final CapturingSourceOutput<String> output = new CapturingSourceOutput<>();
        final KinesisShardSplitState splitState = getTestSplitState();

        KinesisStreamsRecordEmitter<String> emitter =
                new KinesisStreamsRecordEmitter<>(KinesisDeserializationSchema.of(STRING_SCHEMA));
        for (KinesisClientRecord record : inputRecords) {
            emitter.emitRecord(record, output, splitState);
        }

        assertThat(output.getEmittedRecords()).containsExactly("data-1", "data-2", "data-3");
        assertThat(output.getEmittedTimestamps())
                .containsExactly(
                        startTime.toEpochMilli(),
                        startTime.plusSeconds(10).toEpochMilli(),
                        startTime.plusSeconds(20).toEpochMilli());
        assertThat(splitState.getNextStartingPosition())
                .usingRecursiveComparison()
                .isEqualTo(expectedStartingPosition);
    }

    @Test
    void testEmitRecordBasedOnSequenceNumber() throws Exception {
        final Instant startTime = Instant.now();
        List<KinesisClientRecord> inputRecords =
                Stream.of(
                                KinesisClientRecord.builder()
                                        .data(ByteBuffer.wrap(STRING_SCHEMA.serialize("data-1")))
                                        .sequenceNumber("emit")
                                        .approximateArrivalTimestamp(startTime)
                                        .build(),
                                KinesisClientRecord.builder()
                                        .data(ByteBuffer.wrap(STRING_SCHEMA.serialize("data-2")))
                                        .sequenceNumber("emit")
                                        .approximateArrivalTimestamp(startTime.plusSeconds(10))
                                        .build(),
                                KinesisClientRecord.builder()
                                        .data(ByteBuffer.wrap(STRING_SCHEMA.serialize("data-3")))
                                        .approximateArrivalTimestamp(startTime.plusSeconds(20))
                                        .sequenceNumber("do-not-emit")
                                        .build())
                        .collect(Collectors.toList());
        final CapturingSourceOutput<String> output = new CapturingSourceOutput<>();
        final KinesisShardSplitState splitState = getTestSplitState();

        KinesisStreamsRecordEmitter<String> emitter =
                new KinesisStreamsRecordEmitter<>(new SequenceNumberBasedDeserializationSchema());
        for (KinesisClientRecord record : inputRecords) {
            emitter.emitRecord(record, output, splitState);
        }

        assertThat(output.getEmittedRecords()).containsExactly("data-1", "data-2");
        assertThat(output.getEmittedTimestamps())
                .containsExactly(
                        startTime.toEpochMilli(), startTime.plusSeconds(10).toEpochMilli());
    }

    @Test
    void testEmitRecordWithMetadata() throws Exception {
        final Instant startTime = Instant.now();
        List<KinesisClientRecord> inputRecords =
                Stream.of(
                                KinesisClientRecord.builder()
                                        .data(ByteBuffer.wrap(STRING_SCHEMA.serialize("data-1")))
                                        .approximateArrivalTimestamp(startTime)
                                        .build(),
                                KinesisClientRecord.builder()
                                        .data(ByteBuffer.wrap(STRING_SCHEMA.serialize("data-2")))
                                        .approximateArrivalTimestamp(startTime.plusSeconds(10))
                                        .build(),
                                KinesisClientRecord.builder()
                                        .data(ByteBuffer.wrap(STRING_SCHEMA.serialize("data-3")))
                                        .approximateArrivalTimestamp(startTime.plusSeconds(20))
                                        .sequenceNumber("some-sequence-number")
                                        .build())
                        .collect(Collectors.toList());
        final CapturingSourceOutput<String> output = new CapturingSourceOutput<>();
        final KinesisShardSplitState splitState = getTestSplitState();

        KinesisStreamsRecordEmitter<String> emitter =
                new KinesisStreamsRecordEmitter<>(
                        new AssertRecordMetadataDeserializationSchema(
                                splitState.getStreamArn(), splitState.getShardId()));
        for (KinesisClientRecord record : inputRecords) {
            emitter.emitRecord(record, output, splitState);
        }

        assertThat(output.getEmittedRecords()).containsExactly("data-1", "data-2", "data-3");
        assertThat(output.getEmittedTimestamps())
                .containsExactly(
                        startTime.toEpochMilli(),
                        startTime.plusSeconds(10).toEpochMilli(),
                        startTime.plusSeconds(20).toEpochMilli());
    }

    private static class CapturingSourceOutput<T> implements SourceOutput<T> {

        private final List<T> emittedRecords = new ArrayList<>();
        private final List<Long> emittedTimestamps = new ArrayList<>();

        @Override
        public void collect(T record) {
            emittedRecords.add(record);
        }

        @Override
        public void collect(T record, long timestamp) {
            emittedRecords.add(record);
            emittedTimestamps.add(timestamp);
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markIdle() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markActive() {
            throw new UnsupportedOperationException();
        }

        public List<T> getEmittedRecords() {
            return emittedRecords;
        }

        public List<Long> getEmittedTimestamps() {
            return emittedTimestamps;
        }
    }

    private static class SequenceNumberBasedDeserializationSchema
            implements KinesisDeserializationSchema<String> {

        @Override
        public void deserialize(
                KinesisClientRecord record, String stream, String shardId, Collector<String> output)
                throws IOException {
            if (Objects.equals(record.sequenceNumber(), "emit")) {
                ByteBuffer recordData = record.data();
                byte[] dataBytes = new byte[recordData.remaining()];
                recordData.get(dataBytes);
                STRING_SCHEMA.deserialize(dataBytes, output);
            }
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }

    private static class AssertRecordMetadataDeserializationSchema
            implements KinesisDeserializationSchema<String> {
        private final String expectedStreamArn;
        private final String expectedShardId;

        private AssertRecordMetadataDeserializationSchema(
                String expectedStreamArn, String expectedShardId) {
            this.expectedStreamArn = expectedStreamArn;
            this.expectedShardId = expectedShardId;
        }

        @Override
        public void deserialize(
                KinesisClientRecord record, String stream, String shardId, Collector<String> output)
                throws IOException {
            assertThat(stream).isEqualTo(expectedStreamArn);
            assertThat(shardId).isEqualTo(expectedShardId);

            ByteBuffer recordData = record.data();
            byte[] dataBytes = new byte[recordData.remaining()];
            recordData.get(dataBytes);
            STRING_SCHEMA.deserialize(dataBytes, output);
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
