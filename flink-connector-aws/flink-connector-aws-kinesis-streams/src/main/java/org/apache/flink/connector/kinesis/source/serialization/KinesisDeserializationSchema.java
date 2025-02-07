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

package org.apache.flink.connector.kinesis.source.serialization;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.util.Collector;

import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.IOException;
import java.io.Serializable;

/**
 * This is a deserialization schema specific for the {@link KinesisStreamsSource}. Different from
 * the basic {@link DeserializationSchema}, this schema offers additional Kinesis-specific
 * information about the record that may be useful to the user application.
 *
 * @param <T> The type created by the deserialization schema.
 */
@PublicEvolving
public interface KinesisDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #deserialize} and thus suitable for one time setup work.
     *
     * <p>The provided {@link DeserializationSchema.InitializationContext} can be used to access
     * additional features such as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     */
    default void open(DeserializationSchema.InitializationContext context) throws Exception {}

    /**
     * Deserializes a Kinesis record. If the record cannot be deserialized, {@code null} may be
     * returned. This informs the Flink Kinesis Consumer to process the Kinesis record without
     * producing any output for it, i.e. effectively "skipping" the record.
     *
     * @param record the Kinesis record to deserialize
     * @param stream the ARN of the Kinesis stream that this record was sent to
     * @param shardId the identifier of the shard the record was sent to
     * @param output the identifier of the shard the record was sent to
     * @throws IOException exception when deserializing record
     */
    void deserialize(KinesisClientRecord record, String stream, String shardId, Collector<T> output)
            throws IOException;

    static <T> KinesisDeserializationSchema<T> of(DeserializationSchema<T> deserializationSchema) {
        return new KinesisDeserializationSchemaWrapper<>(deserializationSchema);
    }
}
