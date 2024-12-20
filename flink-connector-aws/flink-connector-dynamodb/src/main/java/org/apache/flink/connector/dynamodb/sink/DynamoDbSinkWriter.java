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

package org.apache.flink.connector.dynamodb.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.dynamodb.sink.client.SdkClientProvider;
import org.apache.flink.connector.dynamodb.util.PrimaryKeyBuilder;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.CollectionUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.singletonMap;
import static org.apache.flink.connector.aws.util.AWSCredentialFatalExceptionClassifiers.getInvalidCredentialsExceptionClassifier;
import static org.apache.flink.connector.aws.util.AWSCredentialFatalExceptionClassifiers.getSdkClientMisconfiguredExceptionClassifier;
import static org.apache.flink.connector.base.sink.writer.AsyncSinkFatalExceptionClassifiers.getInterruptedExceptionClassifier;

/**
 * Sink writer created by {@link DynamoDbSink} to write to DynamoDB. More details on the operation
 * of this sink writer may be found in the doc for {@link DynamoDbSink}. More details on the
 * internals of this sink writer may be found in {@link AsyncSinkWriter}.
 *
 * <p>The {@link DynamoDbAsyncClient} used here may be configured in the standard way for the AWS
 * SDK 2.x. e.g. the provision of {@code AWS_REGION}, {@code AWS_ACCESS_KEY_ID} and {@code
 * AWS_SECRET_ACCESS_KEY} through environment variables etc.
 */
@Internal
class DynamoDbSinkWriter<InputT> extends AsyncSinkWriter<InputT, DynamoDbWriteRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbSinkWriter.class);

    private static final FatalExceptionClassifier RESOURCE_NOT_FOUND_EXCEPTION_CLASSIFIER =
            FatalExceptionClassifier.withRootCauseOfType(
                    ResourceNotFoundException.class,
                    err ->
                            new DynamoDbSinkException(
                                    "Encountered non-recoverable exception relating to not being able to find the specified resources",
                                    err));

    private static final FatalExceptionClassifier CONDITIONAL_CHECK_FAILED_EXCEPTION_CLASSIFIER =
            FatalExceptionClassifier.withRootCauseOfType(
                    ConditionalCheckFailedException.class,
                    err ->
                            new DynamoDbSinkException(
                                    "Encountered non-recoverable exception relating to failed conditional check",
                                    err));

    /* Validation exceptions are not retryable. See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html. */
    private static final FatalExceptionClassifier VALIDATION_EXCEPTION_CLASSIFIER =
            new FatalExceptionClassifier(
                    (err) ->
                            err instanceof DynamoDbException
                                    && ((DynamoDbException) err)
                                            .awsErrorDetails()
                                            .errorCode()
                                            .equalsIgnoreCase("ValidationException"),
                    err ->
                            new DynamoDbSinkException(
                                    "Encountered non-recoverable exception because of DynamoDB request validation",
                                    err));

    private static final FatalExceptionClassifier DYNAMODB_FATAL_EXCEPTION_CLASSIFIER =
            FatalExceptionClassifier.createChain(
                    getInterruptedExceptionClassifier(),
                    getInvalidCredentialsExceptionClassifier(),
                    RESOURCE_NOT_FOUND_EXCEPTION_CLASSIFIER,
                    CONDITIONAL_CHECK_FAILED_EXCEPTION_CLASSIFIER,
                    VALIDATION_EXCEPTION_CLASSIFIER,
                    getSdkClientMisconfiguredExceptionClassifier());

    /* A counter for the total number of records that have encountered an error during put */
    private final Counter numRecordsSendErrorsCounter;

    /* A counter for the total number of records that were returned by DynamoDB as unprocessed and were retried */
    private final Counter numRecordsSendPartialFailure;

    /* The sink writer metric group */
    private final SinkWriterMetricGroup metrics;

    private final SdkClientProvider<DynamoDbAsyncClient> clientProvider;
    private final boolean failOnError;
    private final String tableName;

    private final List<String> overwriteByPartitionKeys;

    public DynamoDbSinkWriter(
            ElementConverter<InputT, DynamoDbWriteRequest> elementConverter,
            InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            boolean failOnError,
            String tableName,
            List<String> overwriteByPartitionKeys,
            SdkClientProvider<DynamoDbAsyncClient> clientProvider,
            Collection<BufferedRequestState<DynamoDbWriteRequest>> states) {
        super(
                elementConverter,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes,
                states);
        this.failOnError = failOnError;
        this.tableName = tableName;
        this.overwriteByPartitionKeys = overwriteByPartitionKeys;
        this.metrics = context.metricGroup();
        this.numRecordsSendErrorsCounter = metrics.getNumRecordsSendErrorsCounter();
        this.numRecordsSendPartialFailure = metrics.counter("numRecordsSendPartialFailure");
        this.clientProvider = clientProvider;
    }

    @Override
    protected void submitRequestEntries(
            List<DynamoDbWriteRequest> requestEntries,
            ResultHandler<DynamoDbWriteRequest> resultHandler) {

        List<WriteRequest> items = new ArrayList<>();

        if (CollectionUtil.isNullOrEmpty(overwriteByPartitionKeys)) {
            for (DynamoDbWriteRequest request : requestEntries) {
                items.add(convertToWriteRequest(request));
            }
        } else {
            // deduplication needed
            Map<String, WriteRequest> container = new HashMap<>();
            PrimaryKeyBuilder keyBuilder = new PrimaryKeyBuilder(overwriteByPartitionKeys);
            for (DynamoDbWriteRequest request : requestEntries) {
                WriteRequest req = convertToWriteRequest(request);
                container.put(keyBuilder.build(req), req);
            }
            items.addAll(container.values());
        }

        CompletableFuture<BatchWriteItemResponse> future =
                clientProvider
                        .getClient()
                        .batchWriteItem(
                                BatchWriteItemRequest.builder()
                                        .requestItems(singletonMap(tableName, items))
                                        .build());

        future.whenComplete(
                (response, err) -> {
                    if (err != null) {
                        handleFullyFailedRequest(err, requestEntries, resultHandler);
                    } else if (!CollectionUtil.isNullOrEmpty(response.unprocessedItems())) {
                        handlePartiallyUnprocessedRequest(response, resultHandler);
                    } else {
                        resultHandler.complete();
                    }
                });
    }

    private void handlePartiallyUnprocessedRequest(
            BatchWriteItemResponse response, ResultHandler<DynamoDbWriteRequest> resultHandler) {
        List<DynamoDbWriteRequest> unprocessed = new ArrayList<>();

        for (WriteRequest writeRequest : response.unprocessedItems().get(tableName)) {
            unprocessed.add(convertToDynamoDbWriteRequest(writeRequest));
        }

        LOG.warn("DynamoDB Sink failed to persist and will retry {} entries.", unprocessed.size());
        numRecordsSendErrorsCounter.inc(unprocessed.size());
        numRecordsSendPartialFailure.inc(unprocessed.size());

        resultHandler.retryForEntries(unprocessed);
    }

    private void handleFullyFailedRequest(
            Throwable err,
            List<DynamoDbWriteRequest> requestEntries,
            ResultHandler<DynamoDbWriteRequest> resultHandler) {
        LOG.warn(
                "DynamoDB Sink failed to persist and will retry {} entries.",
                requestEntries.size(),
                err);
        numRecordsSendErrorsCounter.inc(requestEntries.size());

        if (isRetryable(err.getCause(), resultHandler)) {
            resultHandler.retryForEntries(requestEntries);
        }
    }

    private boolean isRetryable(Throwable err, ResultHandler<DynamoDbWriteRequest> resultHandler) {
        // isFatal() is really isNotFatal()
        if (!DYNAMODB_FATAL_EXCEPTION_CLASSIFIER.isFatal(
                err, resultHandler::completeExceptionally)) {
            return false;
        }
        if (failOnError) {
            resultHandler.completeExceptionally(
                    new DynamoDbSinkException.DynamoDbSinkFailFastException(err));
            return false;
        }

        return true;
    }

    @Override
    protected long getSizeInBytes(DynamoDbWriteRequest requestEntry) {
        // dynamodb calculates item size as a sum of all attributes and all values, to calculate it
        // correctly would be an expensive operation and we would potentially be serializing each
        // record twice this can be removed after FLINK-29854 is implemented
        return 0;
    }

    @Override
    public void close() {
        AWSGeneralUtil.closeResources(clientProvider);
    }

    private WriteRequest convertToWriteRequest(DynamoDbWriteRequest dynamoDbWriteRequest) {
        if (dynamoDbWriteRequest.getType() == DynamoDbWriteRequestType.PUT) {
            return WriteRequest.builder()
                    .putRequest(PutRequest.builder().item(dynamoDbWriteRequest.getItem()).build())
                    .build();
        } else if (dynamoDbWriteRequest.getType() == DynamoDbWriteRequestType.DELETE) {
            return WriteRequest.builder()
                    .deleteRequest(
                            DeleteRequest.builder().key(dynamoDbWriteRequest.getItem()).build())
                    .build();
        } else {
            throw new IllegalArgumentException(
                    "Unsupported DynamoDb Write Request Type. consider updating the convertToWriteRequest method");
        }
    }

    private DynamoDbWriteRequest convertToDynamoDbWriteRequest(WriteRequest writeRequest) {
        if (writeRequest.putRequest() != null) {
            return DynamoDbWriteRequest.builder()
                    .setItem(writeRequest.putRequest().item())
                    .setType(DynamoDbWriteRequestType.PUT)
                    .build();
        } else if (writeRequest.deleteRequest() != null) {
            return DynamoDbWriteRequest.builder()
                    .setItem(writeRequest.deleteRequest().key())
                    .setType(DynamoDbWriteRequestType.DELETE)
                    .build();
        } else {
            throw new IllegalArgumentException(
                    "Unsupported Write Request, consider updating the convertToDynamoDbWriteRequest method");
        }
    }
}
