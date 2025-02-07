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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.kinesis.source.serialization.KinesisDeserializationSchema;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.util.Collector;

import software.amazon.kinesis.retrieval.KinesisClientRecord;

/**
 * Emits record from the source into the Flink job graph. This serves as the interface between the
 * source and the Flink job.
 *
 * @param <T> the data type being emitted into the Flink job graph
 */
@Internal
public class KinesisStreamsRecordEmitter<T>
        implements RecordEmitter<KinesisClientRecord, T, KinesisShardSplitState> {

    private final KinesisDeserializationSchema<T> deserializationSchema;
    private final SourceOutputWrapper<T> sourceOutputWrapper = new SourceOutputWrapper<>();

    public KinesisStreamsRecordEmitter(KinesisDeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void emitRecord(
            KinesisClientRecord element, SourceOutput<T> output, KinesisShardSplitState splitState)
            throws Exception {
        sourceOutputWrapper.setSourceOutput(output);
        sourceOutputWrapper.setTimestamp(element.approximateArrivalTimestamp().toEpochMilli());
        deserializationSchema.deserialize(
                element, splitState.getStreamArn(), splitState.getShardId(), sourceOutputWrapper);
        splitState.setNextStartingPosition(
                StartingPosition.continueFromSequenceNumber(element.sequenceNumber()));
    }

    private static class SourceOutputWrapper<T> implements Collector<T> {
        private SourceOutput<T> sourceOutput;
        private long timestamp;

        @Override
        public void collect(T record) {
            sourceOutput.collect(record, timestamp);
        }

        @Override
        public void close() {
            // no-op
        }

        private void setSourceOutput(SourceOutput<T> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }

        private void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }
}
