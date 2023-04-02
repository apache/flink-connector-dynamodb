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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout.StreamConsumerRegistrar;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.EAGER;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_CONSUMER_NAME;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_REGISTRATION_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RECORD_PUBLISHER_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType.EFO;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.efoConsumerArn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/** Tests for {@link StreamConsumerRegistrar}. */
public class StreamConsumerRegistrarUtilTest {
    private static final String STREAM_1_ARN = "arn:aws:kinesis:us-east-1:123456789012:stream/stream-1";
    private static final String STREAM_2_ARN = "arn:aws:kinesis:us-east-1:123456789012:stream/stream-2";

    @Test
    public void testRegisterStreamConsumers() throws Exception {
        Properties configProps = getDefaultConfiguration();
        StreamConsumerRegistrar registrar = mock(StreamConsumerRegistrar.class);
        when(registrar.registerStreamConsumer(STREAM_1_ARN, "consumer-name"))
                .thenReturn("stream-1-consumer-arn");
        when(registrar.registerStreamConsumer(STREAM_2_ARN, "consumer-name"))
                .thenReturn("stream-2-consumer-arn");

        StreamConsumerRegistrarUtil.registerStreamConsumers(
                registrar, configProps, Arrays.asList(STREAM_1_ARN, STREAM_2_ARN));

        assertThat(configProps.getProperty(efoConsumerArn(STREAM_1_ARN)))
                .isEqualTo("stream-1-consumer-arn");
        assertThat(configProps.getProperty(efoConsumerArn(STREAM_2_ARN)))
                .isEqualTo("stream-2-consumer-arn");
    }

    @Test
    public void testDeregisterStreamConsumersMissingStreamArn() throws Exception {
        Properties configProps = getDefaultConfiguration();
        configProps.setProperty(RECORD_PUBLISHER_TYPE, EFO.name());
        List<String> streams = Arrays.asList(STREAM_1_ARN, STREAM_2_ARN);
        StreamConsumerRegistrar registrar = mock(StreamConsumerRegistrar.class);

        StreamConsumerRegistrarUtil.deregisterStreamConsumers(registrar, configProps, streams);

        verify(registrar).deregisterStreamConsumer(STREAM_1_ARN);
        verify(registrar).deregisterStreamConsumer(STREAM_2_ARN);
    }

    @Test
    public void testDeregisterStreamConsumersOnlyDeregistersEFOLazilyInitializedConsumers() {
        Properties configProps = getDefaultConfiguration();
        configProps.setProperty(RECORD_PUBLISHER_TYPE, EFO.name());
        configProps.put(EFO_REGISTRATION_TYPE, EAGER.name());
        List<String> streams = Arrays.asList(STREAM_1_ARN);
        StreamConsumerRegistrar registrar = mock(StreamConsumerRegistrar.class);

        StreamConsumerRegistrarUtil.deregisterStreamConsumers(registrar, configProps, streams);

        verifyZeroInteractions(registrar);
    }

    private Properties getDefaultConfiguration() {
        Properties configProps = new Properties();
        configProps.setProperty(EFO_CONSUMER_NAME, "consumer-name");
        return configProps;
    }
}
