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

package org.apache.flink.connector.sqs.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.aws.sink.throwable.AWSExceptionHandler;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.sqs.sink.client.SdkClientProvider;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.connector.aws.util.AWSCredentialFatalExceptionClassifiers.getInvalidCredentialsExceptionClassifier;
import static org.apache.flink.connector.aws.util.AWSCredentialFatalExceptionClassifiers.getSdkClientMisconfiguredExceptionClassifier;
import static org.apache.flink.connector.base.sink.writer.AsyncSinkFatalExceptionClassifiers.getInterruptedExceptionClassifier;

/**
 * Sink writer created by {@link SqsSink} to write to SQS. More details on the operation of this
 * sink writer may be found in the doc for {@link SqsSink}. More details on the internals of this
 * sink writer may be found in {@link AsyncSinkWriter}.
 *
 * <p>The {@link SqsAsyncClient} used here may be configured in the standard way for the AWS SDK
 * 2.x. e.g. the provision of {@code AWS_REGION}, {@code AWS_ACCESS_KEY_ID} and {@code
 * AWS_SECRET_ACCESS_KEY} through environment variables etc.
 */
@Internal
class SqsSinkWriter<InputT> extends AsyncSinkWriter<InputT, SendMessageBatchRequestEntry> {

    private static final Logger LOG = LoggerFactory.getLogger(SqsSinkWriter.class);

    private final SdkClientProvider<SqsAsyncClient> clientProvider;

    private static final AWSExceptionHandler SQS_EXCEPTION_HANDLER =
            AWSExceptionHandler.withClassifier(
                    FatalExceptionClassifier.createChain(
                            getInterruptedExceptionClassifier(),
                            getInvalidCredentialsExceptionClassifier(),
                            SqsExceptionClassifiers.getResourceNotFoundExceptionClassifier(),
                            SqsExceptionClassifiers.getAccessDeniedExceptionClassifier(),
                            SqsExceptionClassifiers.getNotAuthorizedExceptionClassifier(),
                            getSdkClientMisconfiguredExceptionClassifier()));

    private final Counter numRecordsOutErrorsCounter;

    /* Url of SQS */
    private final String sqsUrl;

    /* The sink writer metric group */
    private final SinkWriterMetricGroup metrics;

    /* Flag to whether fatally fail any time we encounter an exception when persisting records */
    private final boolean failOnError;

    SqsSinkWriter(
            ElementConverter<InputT, SendMessageBatchRequestEntry> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            boolean failOnError,
            String sqsUrl,
            SdkClientProvider<SqsAsyncClient> clientProvider,
            Collection<BufferedRequestState<SendMessageBatchRequestEntry>> initialStates) {
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
        this.sqsUrl = sqsUrl;
        this.metrics = context.metricGroup();
        this.numRecordsOutErrorsCounter = metrics.getNumRecordsOutErrorsCounter();
        this.clientProvider = clientProvider;
    }

    @Override
    protected void submitRequestEntries(
            List<SendMessageBatchRequestEntry> requestEntries,
            ResultHandler<SendMessageBatchRequestEntry> resultHandler) {

        final SendMessageBatchRequest batchRequest =
                SendMessageBatchRequest.builder().entries(requestEntries).queueUrl(sqsUrl).build();

        CompletableFuture<SendMessageBatchResponse> future =
                clientProvider.getClient().sendMessageBatch(batchRequest);

        future.whenComplete(
                        (response, err) -> {
                            if (err != null) {
                                handleFullyFailedRequest(err, requestEntries, resultHandler);
                            } else if (response.failed() != null && !response.failed().isEmpty()) {
                                handlePartiallyFailedRequest(
                                        response, requestEntries, resultHandler);
                            } else {
                                resultHandler.complete();
                            }
                        })
                .exceptionally(
                        ex -> {
                            resultHandler.completeExceptionally(
                                    new SqsSinkException.SqsFailFastSinkException(
                                            ex.getMessage(), ex));
                            return null;
                        });
    }

    @Override
    protected long getSizeInBytes(SendMessageBatchRequestEntry requestEntry) {
        return requestEntry.messageBody().getBytes(StandardCharsets.UTF_8).length;
    }

    @Override
    public void close() {
        AWSGeneralUtil.closeResources(clientProvider);
    }

    private void handleFullyFailedRequest(
            Throwable err,
            List<SendMessageBatchRequestEntry> requestEntries,
            ResultHandler<SendMessageBatchRequestEntry> resultHandler) {

        numRecordsOutErrorsCounter.inc(requestEntries.size());
        boolean isFatal =
                SQS_EXCEPTION_HANDLER.consumeIfFatal(err, resultHandler::completeExceptionally);
        if (isFatal) {
            return;
        }

        if (failOnError) {
            resultHandler.completeExceptionally(new SqsSinkException.SqsFailFastSinkException(err));
            return;
        }

        LOG.warn(
                "SQS Sink failed to write and will retry {} entries to SQS,  First request was {}",
                requestEntries.size(),
                requestEntries.get(0).toString(),
                err);
        resultHandler.retryForEntries(requestEntries);
    }

    private void handlePartiallyFailedRequest(
            SendMessageBatchResponse response,
            List<SendMessageBatchRequestEntry> requestEntries,
            ResultHandler<SendMessageBatchRequestEntry> resultHandler) {

        LOG.warn(
                "handlePartiallyFailedRequest: SQS Sink failed to write and will retry {} entries to SQS",
                response.failed().size());
        numRecordsOutErrorsCounter.inc(response.failed().size());

        if (failOnError) {
            resultHandler.completeExceptionally(new SqsSinkException.SqsFailFastSinkException());
            return;
        }

        final List<SendMessageBatchRequestEntry> failedRequestEntries =
                new ArrayList<>(response.failed().size());

        for (final BatchResultErrorEntry failedEntry : response.failed()) {
            final Optional<SendMessageBatchRequestEntry> retryEntry =
                    getFailedRecord(requestEntries, failedEntry.id());
            if (retryEntry.isPresent()) {
                failedRequestEntries.add(retryEntry.get());
            } else {
                LOG.error(
                        "handlePartiallyFailedRequest: SQS Sink failed to retry unsuccessful SQS publish request due to invalid failed requestId");
                resultHandler.completeExceptionally(
                        new SqsSinkException.SqsFailFastSinkException(
                                "SQS Sink failed to retry unsuccessful SQS publish request due to invalid failed requestId"));
                return;
            }
        }

        resultHandler.retryForEntries(failedRequestEntries);
    }

    private Optional<SendMessageBatchRequestEntry> getFailedRecord(
            List<SendMessageBatchRequestEntry> requestEntries, String selectedId) {
        for (SendMessageBatchRequestEntry entry : requestEntries) {
            if (entry.id().equals(selectedId)) {
                return Optional.of(entry);
            }
        }
        return Optional.empty();
    }
}
