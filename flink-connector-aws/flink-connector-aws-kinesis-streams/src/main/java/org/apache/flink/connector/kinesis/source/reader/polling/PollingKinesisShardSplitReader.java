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

package org.apache.flink.connector.kinesis.source.reader.polling;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.connector.kinesis.source.metrics.KinesisShardMetrics;
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.connector.kinesis.source.reader.KinesisShardSplitReaderBase;
import org.apache.flink.connector.kinesis.source.reader.RecordBatch;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;

import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;

import java.util.Map;

/**
 * An implementation of the KinesisShardSplitReader that periodically polls the Kinesis stream to
 * retrieve records.
 */
@Internal
public class PollingKinesisShardSplitReader extends KinesisShardSplitReaderBase {
    private final StreamProxy kinesis;
    private final Configuration configuration;
    private final int maxRecordsToGet;

    public PollingKinesisShardSplitReader(
            StreamProxy kinesisProxy,
            Map<String, KinesisShardMetrics> shardMetricGroupMap,
            Configuration configuration) {
        super(shardMetricGroupMap);
        this.kinesis = kinesisProxy;
        this.configuration = configuration;
        this.maxRecordsToGet = configuration.get(KinesisSourceConfigOptions.SHARD_GET_RECORDS_MAX);
    }

    @Override
    protected RecordBatch fetchRecords(KinesisShardSplitState splitState) {
        GetRecordsResponse getRecordsResponse =
                kinesis.getRecords(
                        splitState.getStreamArn(),
                        splitState.getShardId(),
                        splitState.getNextStartingPosition(),
                        this.maxRecordsToGet);
        boolean isCompleted = getRecordsResponse.nextShardIterator() == null;

        return new RecordBatch(
                getRecordsResponse.records(),
                splitState.getKinesisShardSplit(),
                getRecordsResponse.millisBehindLatest(),
                isCompleted);
    }

    @Override
    public void close() throws Exception {
        kinesis.close();
    }
}
