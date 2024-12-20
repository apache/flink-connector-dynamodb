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

package org.apache.flink.connector.firehose.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.aws.sink.throwable.AWSExceptionHandler;
import org.apache.flink.connector.aws.util.AWSClientUtil;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchRequest;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponse;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponseEntry;
import software.amazon.awssdk.services.firehose.model.Record;

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
 * Sink writer created by {@link KinesisFirehoseSink} to write to Kinesis Data Firehose. More
 * details on the operation of this sink writer may be found in the doc for {@link
 * KinesisFirehoseSink}. More details on the internals of this sink writer may be found in {@link
 * AsyncSinkWriter}.
 *
 * <p>The {@link FirehoseAsyncClient} used here may be configured in the standard way for the AWS
 * SDK 2.x. e.g. the provision of {@code AWS_REGION}, {@code AWS_ACCESS_KEY_ID} and {@code
 * AWS_SECRET_ACCESS_KEY} through environment variables etc.
 */
@Internal
class KinesisFirehoseSinkWriter<InputT> extends AsyncSinkWriter<InputT, Record> {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisFirehoseSinkWriter.class);

    private static SdkAsyncHttpClient createHttpClient(Properties firehoseClientProperties) {
        return AWSGeneralUtil.createAsyncHttpClient(firehoseClientProperties);
    }

    private static FirehoseAsyncClient createFirehoseClient(
            Properties firehoseClientProperties, SdkAsyncHttpClient httpClient) {
        AWSGeneralUtil.validateAwsCredentials(firehoseClientProperties);
        return AWSClientUtil.createAwsAsyncClient(
                firehoseClientProperties,
                httpClient,
                FirehoseAsyncClient.builder(),
                KinesisFirehoseConfigConstants.BASE_FIREHOSE_USER_AGENT_PREFIX_FORMAT,
                KinesisFirehoseConfigConstants.FIREHOSE_CLIENT_USER_AGENT_PREFIX);
    }

    private static final AWSExceptionHandler FIREHOSE_EXCEPTION_HANDLER =
            AWSExceptionHandler.withClassifier(
                    FatalExceptionClassifier.createChain(
                            getInterruptedExceptionClassifier(),
                            getInvalidCredentialsExceptionClassifier(),
                            AWSFirehoseExceptionClassifiers
                                    .getResourceNotFoundExceptionClassifier(),
                            AWSFirehoseExceptionClassifiers.getAccessDeniedExceptionClassifier(),
                            AWSFirehoseExceptionClassifiers.getNotAuthorizedExceptionClassifier(),
                            getSdkClientMisconfiguredExceptionClassifier()));

    private final Counter numRecordsOutErrorsCounter;

    /* Name of the delivery stream in Kinesis Data Firehose */
    private final String deliveryStreamName;

    /* The sink writer metric group */
    private final SinkWriterMetricGroup metrics;

    /* The asynchronous http client */
    private final SdkAsyncHttpClient httpClient;

    /* The asynchronous Firehose client */
    private final FirehoseAsyncClient firehoseClient;

    /* Flag to whether fatally fail any time we encounter an exception when persisting records */
    private final boolean failOnError;

    KinesisFirehoseSinkWriter(
            ElementConverter<InputT, Record> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            boolean failOnError,
            String deliveryStreamName,
            Properties firehoseClientProperties) {
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
                deliveryStreamName,
                firehoseClientProperties,
                Collections.emptyList());
    }

    KinesisFirehoseSinkWriter(
            ElementConverter<InputT, Record> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            boolean failOnError,
            String deliveryStreamName,
            Properties firehoseClientProperties,
            Collection<BufferedRequestState<Record>> initialStates) {
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
                        .build(),
                initialStates);
        this.failOnError = failOnError;
        this.deliveryStreamName = deliveryStreamName;
        this.metrics = context.metricGroup();
        this.numRecordsOutErrorsCounter = metrics.getNumRecordsOutErrorsCounter();
        this.httpClient = createHttpClient(firehoseClientProperties);
        this.firehoseClient = createFirehoseClient(firehoseClientProperties, httpClient);
    }

    @Override
    protected void submitRequestEntries(
            List<Record> requestEntries, ResultHandler<Record> resultHandler) {

        PutRecordBatchRequest batchRequest =
                PutRecordBatchRequest.builder()
                        .records(requestEntries)
                        .deliveryStreamName(deliveryStreamName)
                        .build();

        CompletableFuture<PutRecordBatchResponse> future =
                firehoseClient.putRecordBatch(batchRequest);

        future.whenComplete(
                (response, err) -> {
                    if (err != null) {
                        handleFullyFailedRequest(err, requestEntries, resultHandler);
                    } else if (response.failedPutCount() > 0) {
                        handlePartiallyFailedRequest(response, requestEntries, resultHandler);
                    } else {
                        resultHandler.complete();
                    }
                });
    }

    @Override
    protected long getSizeInBytes(Record requestEntry) {
        return requestEntry.data().asByteArrayUnsafe().length;
    }

    @Override
    public void close() {
        AWSGeneralUtil.closeResources(httpClient, firehoseClient);
    }

    private void handleFullyFailedRequest(
            Throwable err, List<Record> requestEntries, ResultHandler<Record> resultHandler) {

        numRecordsOutErrorsCounter.inc(requestEntries.size());
        boolean isFatal =
                FIREHOSE_EXCEPTION_HANDLER.consumeIfFatal(
                        err, resultHandler::completeExceptionally);
        if (isFatal) {
            return;
        }

        if (failOnError) {
            resultHandler.completeExceptionally(
                    new KinesisFirehoseException.KinesisFirehoseFailFastException(err));
            return;
        }

        LOG.warn(
                "KDF Sink failed to write and will retry {} entries to KDF",
                requestEntries.size(),
                err);
        resultHandler.retryForEntries(requestEntries);
    }

    private void handlePartiallyFailedRequest(
            PutRecordBatchResponse response,
            List<Record> requestEntries,
            ResultHandler<Record> resultHandler) {
        numRecordsOutErrorsCounter.inc(response.failedPutCount());
        if (failOnError) {
            resultHandler.completeExceptionally(
                    new KinesisFirehoseException.KinesisFirehoseFailFastException());
            return;
        }

        LOG.warn(
                "KDF Sink failed to write and will retry {} entries to KDF", requestEntries.size());
        List<Record> failedRequestEntries = new ArrayList<>(response.failedPutCount());
        List<PutRecordBatchResponseEntry> records = response.requestResponses();

        for (int i = 0; i < records.size(); i++) {
            if (records.get(i).errorCode() != null) {
                failedRequestEntries.add(requestEntries.get(i));
            }
        }

        resultHandler.retryForEntries(failedRequestEntries);
    }
}
