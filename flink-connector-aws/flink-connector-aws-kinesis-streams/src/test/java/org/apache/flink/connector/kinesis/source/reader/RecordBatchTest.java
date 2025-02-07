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

import org.apache.flink.connector.kinesis.source.util.TestUtil;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestRecord;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.Assertions.assertThat;

class RecordBatchTest {

    @Test
    public void testDeaggregateRecordsPassThrough() {
        List<Record> records =
                Stream.of(getTestRecord("data-1"), getTestRecord("data-2"), getTestRecord("data-3"))
                        .collect(Collectors.toList());

        RecordBatch result = new RecordBatch(records, getTestSplit(), 100L, true);

        assertThat(result.getDeaggregatedRecords().size()).isEqualTo(3);
    }

    @Test
    public void testDeaggregateRecordsWithAggregatedRecords() {
        List<Record> records =
                Stream.of(getTestRecord("data-1"), getTestRecord("data-2"), getTestRecord("data-3"))
                        .collect(Collectors.toList());

        Record aggregatedRecord = TestUtil.createAggregatedRecord(records);

        RecordBatch result =
                new RecordBatch(
                        Collections.singletonList(aggregatedRecord), getTestSplit(), 100L, true);

        assertThat(result.getDeaggregatedRecords().size()).isEqualTo(3);
    }

    @Test
    public void testGetMillisBehindLatest() {
        RecordBatch result =
                new RecordBatch(
                        Collections.singletonList(getTestRecord("data-1")),
                        getTestSplit(),
                        100L,
                        true);

        assertThat(result.getMillisBehindLatest()).isEqualTo(100L);
    }

    @Test
    public void testIsCompleted() {
        RecordBatch result =
                new RecordBatch(
                        Collections.singletonList(getTestRecord("data-1")),
                        getTestSplit(),
                        100L,
                        true);

        assertThat(result.isCompleted()).isTrue();
    }
}
