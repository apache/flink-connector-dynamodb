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

package org.apache.flink.connector.kinesis.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.aws.util.AWSClientUtil;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.base.sink.writer.strategy.AIMDScalingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.CongestionControlRateLimitingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.RateLimitingStrategy;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.connector.aws.util.AWSCredentialFatalExceptionClassifiers.getInvalidCredentialsExceptionClassifier;
import static org.apache.flink.connector.aws.util.AWSCredentialFatalExceptionClassifiers.getSdkClientMisconfiguredExceptionClassifier;
import static org.apache.flink.connector.base.sink.writer.AsyncSinkFatalExceptionClassifiers.getInterruptedExceptionClassifier;

/**
 * Sink writer created by {@link KinesisStreamsSink} to write to Kinesis Data Streams. More details
 * on the operation of this sink writer may be found in the doc for {@link KinesisStreamsSink}. More
 * details on the internals of this sink writer may be found in {@link AsyncSinkWriter}.
 *
 * <p>The {@link KinesisAsyncClient} used here may be configured in the standard way for the AWS SDK
 * 2.x. e.g. the provision of {@code AWS_REGION}, {@code AWS_ACCESS_KEY_ID} and {@code
 * AWS_SECRET_ACCESS_KEY} through environment variables etc.
 */
@Internal
class KinesisStreamsSinkWriter<InputT> extends AsyncSinkWriter<InputT, PutRecordsRequestEntry> {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisStreamsSinkWriter.class);

    private static final FatalExceptionClassifier RESOURCE_NOT_FOUND_EXCEPTION_CLASSIFIER =
            FatalExceptionClassifier.withRootCauseOfType(
                    ResourceNotFoundException.class,
                    err ->
                            new KinesisStreamsException(
                                    "Encountered non-recoverable exception relating to not being able to find the specified resources",
                                    err));

    private static final FatalExceptionClassifier KINESIS_FATAL_EXCEPTION_CLASSIFIER =
            FatalExceptionClassifier.createChain(
                    getInterruptedExceptionClassifier(),
                    getInvalidCredentialsExceptionClassifier(),
                    RESOURCE_NOT_FOUND_EXCEPTION_CLASSIFIER,
                    getSdkClientMisconfiguredExceptionClassifier());

    private static final int AIMD_RATE_LIMITING_STRATEGY_INCREASE_RATE = 10;
    private static final double AIMD_RATE_LIMITING_STRATEGY_DECREASE_FACTOR = 0.99D;

    private final Counter numRecordsOutErrorsCounter;

    /* Name of the stream in Kinesis Data Streams */
    private final String streamName;

    /* ARN of the stream in Kinesis Data Streams */
    private final String streamArn;

    /* The sink writer metric group */
    private final SinkWriterMetricGroup metrics;

    /* The asynchronous http client for the asynchronous Kinesis client */
    private final SdkAsyncHttpClient httpClient;

    /* The asynchronous Kinesis client - construction is by kinesisClientProperties */
    private final KinesisAsyncClient kinesisClient;

    /* Flag to whether fatally fail any time we encounter an exception when persisting records */
    private final boolean failOnError;

    KinesisStreamsSinkWriter(
            ElementConverter<InputT, PutRecordsRequestEntry> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            boolean failOnError,
            String streamName,
            String streamArn,
            Properties kinesisClientProperties) {
        this(
                elementConverter,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes,
                failOnError,
                streamName,
                streamArn,
                kinesisClientProperties,
                Collections.emptyList());
    }

    KinesisStreamsSinkWriter(
            ElementConverter<InputT, PutRecordsRequestEntry> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            boolean failOnError,
            String streamName,
            String streamArn,
            Properties kinesisClientProperties,
            Collection<BufferedRequestState<PutRecordsRequestEntry>> states) {
        super(
                elementConverter,
                context,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(maxBatchSize)
                        .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                        .setMaxInFlightRequests(maxInFlightRequests)
                        .setMaxBufferedRequests(maxBufferedRequests)
                        .setMaxTimeInBufferMS(maxTimeInBufferMS)
                        .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                        .setRateLimitingStrategy(
                                buildRateLimitingStrategy(maxInFlightRequests, maxBatchSize))
                        .build(),
                states);
        this.failOnError = failOnError;
        this.streamName = streamName;
        this.streamArn = streamArn;
        this.metrics = context.metricGroup();
        this.numRecordsOutErrorsCounter = metrics.getNumRecordsOutErrorsCounter();
        this.httpClient = AWSGeneralUtil.createAsyncHttpClient(kinesisClientProperties);
        this.kinesisClient = buildClient(kinesisClientProperties, this.httpClient);
    }

    private KinesisAsyncClient buildClient(
            Properties kinesisClientProperties, SdkAsyncHttpClient httpClient) {
        AWSGeneralUtil.validateAwsCredentials(kinesisClientProperties);

        return AWSClientUtil.createAwsAsyncClient(
                kinesisClientProperties,
                httpClient,
                KinesisAsyncClient.builder(),
                KinesisStreamsConfigConstants.BASE_KINESIS_USER_AGENT_PREFIX_FORMAT,
                KinesisStreamsConfigConstants.KINESIS_CLIENT_USER_AGENT_PREFIX);
    }

    private static RateLimitingStrategy buildRateLimitingStrategy(
            int maxInFlightRequests, int maxBatchSize) {
        return CongestionControlRateLimitingStrategy.builder()
                .setMaxInFlightRequests(maxInFlightRequests)
                .setInitialMaxInFlightMessages(maxBatchSize)
                .setScalingStrategy(
                        AIMDScalingStrategy.builder(maxBatchSize * maxInFlightRequests)
                                .setIncreaseRate(AIMD_RATE_LIMITING_STRATEGY_INCREASE_RATE)
                                .setDecreaseFactor(AIMD_RATE_LIMITING_STRATEGY_DECREASE_FACTOR)
                                .build())
                .build();
    }

    @Override
    protected void submitRequestEntries(
            List<PutRecordsRequestEntry> requestEntries,
            ResultHandler<PutRecordsRequestEntry> resultHandler) {

        PutRecordsRequest batchRequest =
                PutRecordsRequest.builder()
                        .records(requestEntries)
                        .streamName(streamName)
                        .streamARN(streamArn)
                        .build();

        CompletableFuture<PutRecordsResponse> future = kinesisClient.putRecords(batchRequest);

        future.whenComplete(
                (response, err) -> {
                    if (err != null) {
                        handleFullyFailedRequest(err, requestEntries, resultHandler);
                    } else if (response.failedRecordCount() > 0) {
                        handlePartiallyFailedRequest(response, requestEntries, resultHandler);
                    } else {
                        resultHandler.complete();
                    }
                });
    }

    @Override
    protected long getSizeInBytes(PutRecordsRequestEntry requestEntry) {
        return requestEntry.data().asByteArrayUnsafe().length;
    }

    private void handleFullyFailedRequest(
            Throwable err,
            List<PutRecordsRequestEntry> requestEntries,
            ResultHandler<PutRecordsRequestEntry> resultHandler) {
        LOG.warn(
                "KDS Sink failed to write and will retry {} entries to KDS",
                requestEntries.size(),
                err);
        numRecordsOutErrorsCounter.inc(requestEntries.size());

        if (isRetryable(err, resultHandler)) {
            resultHandler.retryForEntries(requestEntries);
        }
    }

    @Override
    public void close() {
        AWSGeneralUtil.closeResources(httpClient, kinesisClient);
    }

    private void handlePartiallyFailedRequest(
            PutRecordsResponse response,
            List<PutRecordsRequestEntry> requestEntries,
            ResultHandler<PutRecordsRequestEntry> resultHandler) {
        LOG.warn(
                "KDS Sink failed to write and will retry {} entries to KDS",
                response.failedRecordCount());
        numRecordsOutErrorsCounter.inc(response.failedRecordCount());

        if (failOnError) {
            resultHandler.completeExceptionally(
                    new KinesisStreamsException.KinesisStreamsFailFastException());
            return;
        }
        List<PutRecordsRequestEntry> failedRequestEntries =
                new ArrayList<>(response.failedRecordCount());
        List<PutRecordsResultEntry> records = response.records();

        for (int i = 0; i < records.size(); i++) {
            if (records.get(i).errorCode() != null) {
                failedRequestEntries.add(requestEntries.get(i));
            }
        }

        resultHandler.retryForEntries(failedRequestEntries);
    }

    private boolean isRetryable(
            Throwable err, ResultHandler<PutRecordsRequestEntry> resultHandler) {

        if (!KINESIS_FATAL_EXCEPTION_CLASSIFIER.isFatal(
                err, resultHandler::completeExceptionally)) {
            return false;
        }
        if (failOnError) {
            resultHandler.completeExceptionally(
                    new KinesisStreamsException.KinesisStreamsFailFastException(err));
            return false;
        }

        return true;
    }
}
