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
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;

import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Dataclass to store a batch of Kinesis records with metadata. Used to pass Kinesis records from
 * the SplitReader implementation to the SplitReaderBase.
 *
 * <p>Input records are de-aggregated using KCL 3.x library. It is expected that AWS SDK v2.x
 * messages are converted to KCL 3.x {@link KinesisClientRecord}.
 */
@Internal
public class RecordBatch {
    private final List<KinesisClientRecord> deaggregatedRecords;
    private final long millisBehindLatest;
    private final boolean completed;

    public RecordBatch(
            final List<Record> records,
            final KinesisShardSplit subscribedShard,
            final long millisBehindLatest,
            final boolean completed) {
        this.deaggregatedRecords = deaggregateRecords(records, subscribedShard);
        this.millisBehindLatest = millisBehindLatest;
        this.completed = completed;
    }

    public List<KinesisClientRecord> getDeaggregatedRecords() {
        return deaggregatedRecords;
    }

    public long getMillisBehindLatest() {
        return millisBehindLatest;
    }

    public boolean isCompleted() {
        return completed;
    }

    private List<KinesisClientRecord> deaggregateRecords(
            final List<Record> records, final KinesisShardSplit subscribedShard) {
        final List<KinesisClientRecord> kinesisClientRecords = new ArrayList<>();
        for (Record record : records) {
            kinesisClientRecords.add(KinesisClientRecord.fromRecord(record));
        }

        final String startingHashKey = subscribedShard.getStartingHashKey();
        final String endingHashKey = subscribedShard.getEndingHashKey();

        return new AggregatorUtil()
                .deaggregate(kinesisClientRecords, startingHashKey, endingHashKey);
    }
}
