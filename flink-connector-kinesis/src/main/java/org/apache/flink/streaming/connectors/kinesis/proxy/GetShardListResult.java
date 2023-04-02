/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.proxy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Basic model class to bundle the shards retrieved from Kinesis on a {@link
 * KinesisProxyInterface#getShardList(Map)} call.
 */
@Internal
public class GetShardListResult {

    private final Map<String, LinkedList<StreamShardHandle>> streamsToRetrievedShardList =
            new HashMap<>();

    public void addRetrievedShardToStream(String streamArn, StreamShardHandle retrievedShard) {
        if (!streamsToRetrievedShardList.containsKey(streamArn)) {
            streamsToRetrievedShardList.put(streamArn, new LinkedList<>());
        }
        streamsToRetrievedShardList.get(streamArn).add(retrievedShard);
    }

    public void addRetrievedShardsToStream(
            String streamArn, List<StreamShardHandle> retrievedShards) {
        if (retrievedShards.size() != 0) {
            if (!streamsToRetrievedShardList.containsKey(streamArn)) {
                streamsToRetrievedShardList.put(streamArn, new LinkedList<>());
            }
            streamsToRetrievedShardList.get(streamArn).addAll(retrievedShards);
        }
    }

    public List<StreamShardHandle> getRetrievedShardListOfStream(String streamArn) {
        return streamsToRetrievedShardList.getOrDefault(streamArn, null);
    }

    public StreamShardHandle getLastSeenShardOfStream(String streamArn) {
        if (!streamsToRetrievedShardList.containsKey(streamArn)) {
            return null;
        } else {
            return streamsToRetrievedShardList.get(streamArn).getLast();
        }
    }

    public boolean hasRetrievedShards() {
        return !streamsToRetrievedShardList.isEmpty();
    }

    public Set<String> getStreamsWithRetrievedShards() {
        return streamsToRetrievedShardList.keySet();
    }
}
