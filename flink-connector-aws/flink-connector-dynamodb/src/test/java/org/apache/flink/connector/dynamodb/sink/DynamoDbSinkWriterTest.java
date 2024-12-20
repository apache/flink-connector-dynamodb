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

import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.connector.dynamodb.sink.client.SdkClientProvider;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbServiceClientConfiguration;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.sts.model.StsException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DynamoDbSinkWriter}. */
public class DynamoDbSinkWriterTest {

    private static final String PARTITION_KEY = "partition_key";
    private static final String SORT_KEY = "sort_key";
    private static final String TABLE_NAME = "table_name";

    @Test
    public void testSuccessfulRequestWithNoDeduplication() throws Exception {
        List<String> overwriteByPartitionKeys = Collections.emptyList();
        List<DynamoDbWriteRequest> inputRequests =
                Arrays.asList(
                        sinkPutRequest(item("pk", "1")),
                        sinkPutRequest(item("pk", "2")),
                        sinkDeleteRequest(item("pk", "3")),
                        sinkDeleteRequest(item("pk", "4")));
        List<WriteRequest> expectedClientRequests =
                Arrays.asList(
                        dynamoDbPutRequest(item("pk", "1")),
                        dynamoDbPutRequest(item("pk", "2")),
                        dynamoDbDeleteRequest(item("pk", "3")),
                        dynamoDbDeleteRequest(item("pk", "4")));

        TrackingDynamoDbAsyncClient trackingDynamoDbAsyncClient = new TrackingDynamoDbAsyncClient();
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        true, overwriteByPartitionKeys, () -> trackingDynamoDbAsyncClient);
        TestingResultHandler resultHandler = new TestingResultHandler();

        dynamoDbSinkWriter.submitRequestEntries(inputRequests, resultHandler);
        assertThat(trackingDynamoDbAsyncClient.getRequestHistory())
                .isNotEmpty()
                .containsExactly(expectedClientRequests);
        assertThat(resultHandler.isComplete()).isTrue();
    }

    @Test
    public void testPutRequestPartitionKeyDeduplication() throws Exception {
        List<String> overwriteByPartitionKeys = singletonList(PARTITION_KEY);
        List<DynamoDbWriteRequest> inputRequests =
                Arrays.asList(sinkPutRequest(item("pk", "1")), sinkPutRequest(item("pk", "2")));
        List<WriteRequest> expectedClientRequests =
                Arrays.asList(dynamoDbPutRequest(item("pk", "2")));

        TrackingDynamoDbAsyncClient trackingDynamoDbAsyncClient = new TrackingDynamoDbAsyncClient();
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        true, overwriteByPartitionKeys, () -> trackingDynamoDbAsyncClient);

        TestingResultHandler resultHandler = new TestingResultHandler();
        dynamoDbSinkWriter.submitRequestEntries(inputRequests, resultHandler);
        assertThat(trackingDynamoDbAsyncClient.getRequestHistory())
                .isNotEmpty()
                .containsExactly(expectedClientRequests);
        assertThat(resultHandler.isComplete()).isTrue();
    }

    @Test
    public void testDeleteRequestPartitionKeyDeduplication() throws Exception {
        List<String> overwriteByPartitionKeys = singletonList(PARTITION_KEY);
        List<DynamoDbWriteRequest> inputRequests =
                Arrays.asList(
                        sinkDeleteRequest(item("pk", "1")), sinkDeleteRequest(item("pk", "2")));
        List<WriteRequest> expectedClientRequests =
                singletonList(dynamoDbDeleteRequest(item("pk", "2")));

        TrackingDynamoDbAsyncClient trackingDynamoDbAsyncClient = new TrackingDynamoDbAsyncClient();
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        true, overwriteByPartitionKeys, () -> trackingDynamoDbAsyncClient);
        TestingResultHandler resultHandler = new TestingResultHandler();

        dynamoDbSinkWriter.submitRequestEntries(inputRequests, resultHandler);
        assertThat(trackingDynamoDbAsyncClient.getRequestHistory())
                .isNotEmpty()
                .containsExactly(expectedClientRequests);
        assertThat(resultHandler.isComplete()).isTrue();
    }

    @Test
    public void testMultipleKeyDeduplication() throws Exception {
        List<String> overwriteByPartitionKeys = Arrays.asList(PARTITION_KEY, SORT_KEY);
        List<DynamoDbWriteRequest> inputRequests =
                Arrays.asList(
                        sinkPutRequest(item("pk", "1")),
                        sinkPutRequest(item("pk", "2")),
                        sinkPutRequest(itemWithPayload("pk", "2", "string_payload_1")),
                        sinkPutRequest(itemWithPayload("pk", "2", "string_payload_2")));
        List<WriteRequest> expectedClientRequests =
                Arrays.asList(
                        dynamoDbPutRequest(item("pk", "1")),
                        dynamoDbPutRequest(itemWithPayload("pk", "2", "string_payload_2")));

        TrackingDynamoDbAsyncClient trackingDynamoDbAsyncClient = new TrackingDynamoDbAsyncClient();
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        true, overwriteByPartitionKeys, () -> trackingDynamoDbAsyncClient);
        TestingResultHandler resultHandler = new TestingResultHandler();

        dynamoDbSinkWriter.submitRequestEntries(inputRequests, resultHandler);
        // Order does not matter in a batch write request
        assertThat(trackingDynamoDbAsyncClient.getRequestHistory())
                .isNotEmpty()
                .allSatisfy(
                        clientBatchRequest ->
                                assertThat(clientBatchRequest)
                                        .containsExactlyInAnyOrderElementsOf(
                                                expectedClientRequests));
        assertThat(resultHandler.isComplete()).isTrue();
    }

    @Test
    public void testRetryableExceptionWhenFailOnErrorOnWillNotRetry() throws IOException {
        Optional<Exception> exceptionToThrow = getGenericRetryableException();
        boolean failOnError = true;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testRetryableExceptionWhenFailOnErrorOffWillRetry() throws Exception {
        Optional<Exception> exceptionToThrow = getGenericRetryableException();
        boolean failOnError = false;

        List<DynamoDbWriteRequest> inputRequests = getDefaultInputRequests();
        ThrowingDynamoDbAsyncClient<Exception> throwingDynamoDbAsyncClient =
                new ThrowingDynamoDbAsyncClient<>(exceptionToThrow, str -> true);
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        failOnError, Collections.emptyList(), () -> throwingDynamoDbAsyncClient);
        TestingResultHandler resultHandler = new TestingResultHandler();

        dynamoDbSinkWriter.submitRequestEntries(inputRequests, resultHandler);

        assertThat(resultHandler.isComplete()).isFalse();
        assertThat(resultHandler.getRetryEntries())
                .containsExactlyInAnyOrderElementsOf(inputRequests);
    }

    @Test
    public void testPartiallyFailedRequestRetriesFailedRecords() throws Exception {
        boolean failOnError = false;
        List<DynamoDbWriteRequest> inputRequests =
                Arrays.asList(
                        sinkPutRequest(item("put_will_fail_pk", "1")),
                        sinkPutRequest(item("put_will_not_fail_pk", "2")),
                        sinkDeleteRequest(item("delete_will_fail_pk", "3")),
                        sinkDeleteRequest(item("delete_will_not_fail_pk", "4")));
        List<DynamoDbWriteRequest> expectedRetriedRecords =
                Arrays.asList(
                        sinkPutRequest(item("put_will_fail_pk", "1")),
                        sinkDeleteRequest(item("delete_will_fail_pk", "3")));
        Predicate<String> failWhenPartitionKeyMatcher = str -> str.contains("will_fail_pk");

        FailingRecordsDynamoDbAsyncClient failingRecordsDynamoDbAsyncClient =
                new FailingRecordsDynamoDbAsyncClient(failWhenPartitionKeyMatcher);
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        failOnError,
                        Collections.emptyList(),
                        () -> failingRecordsDynamoDbAsyncClient);
        TestingResultHandler resultHandler = new TestingResultHandler();

        dynamoDbSinkWriter.submitRequestEntries(inputRequests, resultHandler);

        assertThat(resultHandler.isComplete()).isFalse();
        assertThat(resultHandler.getRetryEntries())
                .usingRecursiveComparison()
                .isEqualTo(expectedRetriedRecords);
    }

    @Test
    public void testNonRetryableExceptionWhenFailOnErrorOnWillNotRetry() throws IOException {
        Optional<Exception> exceptionToThrow = getGenericNonRetryableException();
        boolean failOnError = true;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testNonRetryableExceptionWhenFailOnErrorOffWillNotRetry() throws IOException {
        Optional<Exception> exceptionToThrow = getGenericNonRetryableException();
        boolean failOnError = false;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testInterruptedExceptionIsNonRetryable() throws IOException {
        Optional<Exception> exceptionToThrow = Optional.of(new InterruptedException());
        boolean failOnError = false;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testInvalidCredentialsExceptionIsNonRetryable() throws IOException {
        Optional<Exception> exceptionToThrow = Optional.of(StsException.builder().build());
        boolean failOnError = false;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testResourceNotFoundExceptionIsNonRetryable() throws IOException {
        Optional<Exception> exceptionToThrow =
                Optional.of(ResourceNotFoundException.builder().build());
        boolean failOnError = false;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testConditionalCheckFailedExceptionIsNonRetryable() throws IOException {
        Optional<Exception> exceptionToThrow =
                Optional.of(ConditionalCheckFailedException.builder().build());
        boolean failOnError = false;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testValidationExceptionIsNonRetryable() throws IOException {
        Optional<Exception> exceptionToThrow =
                Optional.of(
                        DynamoDbException.builder()
                                .awsErrorDetails(
                                        AwsErrorDetails.builder()
                                                .errorCode("ValidationException")
                                                .build())
                                .build());
        boolean failOnError = false;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testSdkClientExceptionIsNonRetryable() throws IOException {
        Optional<Exception> exceptionToThrow = Optional.of(SdkClientException.builder().build());
        boolean failOnError = false;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testGetSizeInBytesNotImplemented() throws IOException {
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        false, Collections.emptyList(), () -> new TrackingDynamoDbAsyncClient());
        assertThat(dynamoDbSinkWriter.getSizeInBytes(sinkPutRequest(item("pk", "1")))).isEqualTo(0);
    }

    @Test
    public void testClientClosesWhenWriterIsClosed() throws IOException {
        TestAsyncDynamoDbClientProvider testAsyncDynamoDbClientProvider =
                new TestAsyncDynamoDbClientProvider(new TrackingDynamoDbAsyncClient());
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        false, Collections.emptyList(), testAsyncDynamoDbClientProvider);
        dynamoDbSinkWriter.close();

        assertThat(testAsyncDynamoDbClientProvider.getCloseCount()).isEqualTo(1);
    }

    private void assertThatRequestsAreNotRetried(
            boolean failOnError, Optional<Exception> exceptionToThrow) throws IOException {
        ThrowingDynamoDbAsyncClient<Exception> throwingDynamoDbAsyncClient =
                new ThrowingDynamoDbAsyncClient<>(exceptionToThrow, str -> true);
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        failOnError, Collections.emptyList(), () -> throwingDynamoDbAsyncClient);
        TestingResultHandler resultHandler = new TestingResultHandler();

        dynamoDbSinkWriter.submitRequestEntries(getDefaultInputRequests(), resultHandler);
        assertThat(resultHandler.isComplete()).isFalse();
        assertThat(resultHandler.getRetryEntries()).isEmpty();
        // assert exceptionToThrow is thrown or wrapped
        assertThat(resultHandler.getException())
                .satisfies(
                        e ->
                                assertThat(
                                                (e == exceptionToThrow.get()
                                                        || e.getCause() == exceptionToThrow.get()))
                                        .isTrue());
    }

    private DynamoDbSinkWriter<Map<String, AttributeValue>> getDefaultSinkWriter(
            boolean failOnError,
            List<String> overwriteByPartitionKeys,
            Supplier<DynamoDbAsyncClient> clientSupplier)
            throws IOException {
        return getDefaultSinkWriter(
                failOnError,
                overwriteByPartitionKeys,
                new TestAsyncDynamoDbClientProvider(clientSupplier.get()));
    }

    private DynamoDbSinkWriter<Map<String, AttributeValue>> getDefaultSinkWriter(
            boolean failOnError,
            List<String> overwriteByPartitionKeys,
            SdkClientProvider<DynamoDbAsyncClient> dynamoDbAsyncClientProvider)
            throws IOException {
        TestSinkInitContext initContext = new TestSinkInitContext();
        DynamoDbSink<Map<String, AttributeValue>> sink =
                new DynamoDbSink<>(
                        new TestDynamoDbElementConverter(),
                        2,
                        1,
                        10,
                        1024,
                        1000,
                        1024,
                        failOnError,
                        TABLE_NAME,
                        overwriteByPartitionKeys,
                        new Properties());
        sink.setDynamoDbAsyncClientProvider(dynamoDbAsyncClientProvider);
        return (DynamoDbSinkWriter<Map<String, AttributeValue>>) sink.createWriter(initContext);
    }

    private List<DynamoDbWriteRequest> getDefaultInputRequests() {
        return Arrays.asList(sinkPutRequest(item("pk", "1")), sinkPutRequest(item("pk", "2")));
    }

    private Optional<Exception> getGenericRetryableException() {
        return Optional.of(
                ProvisionedThroughputExceededException.builder()
                        .awsErrorDetails(
                                AwsErrorDetails.builder()
                                        .errorCode("SomeErrorCodeThatIsNotUsed")
                                        .build())
                        .build());
    }

    private Optional<Exception> getGenericNonRetryableException() {
        return Optional.of(
                ResourceNotFoundException.builder()
                        .awsErrorDetails(
                                AwsErrorDetails.builder()
                                        .errorCode("SomeErrorCodeThatIsNotUsed")
                                        .build())
                        .build());
    }

    private DynamoDbWriteRequest sinkPutRequest(Map<String, AttributeValue> item) {
        return DynamoDbWriteRequest.builder()
                .setType(DynamoDbWriteRequestType.PUT)
                .setItem(item)
                .build();
    }

    private DynamoDbWriteRequest sinkDeleteRequest(Map<String, AttributeValue> item) {
        return DynamoDbWriteRequest.builder()
                .setType(DynamoDbWriteRequestType.DELETE)
                .setItem(item)
                .build();
    }

    private WriteRequest dynamoDbPutRequest(Map<String, AttributeValue> item) {
        return WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build();
    }

    private WriteRequest dynamoDbDeleteRequest(Map<String, AttributeValue> item) {
        return WriteRequest.builder()
                .deleteRequest(DeleteRequest.builder().key(item).build())
                .build();
    }

    private Map<String, AttributeValue> item(String partitionKey, String sortKey) {
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put(PARTITION_KEY, AttributeValue.builder().s(partitionKey).build());
        item.put(SORT_KEY, AttributeValue.builder().n(sortKey).build());
        item.put("string_payload", AttributeValue.builder().s("some_strings").build());
        item.put("number_payload", AttributeValue.builder().n("1234").build());
        return item;
    }

    private Map<String, AttributeValue> itemWithPayload(
            String partitionKey, String sortKey, String payload) {
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put(PARTITION_KEY, AttributeValue.builder().s(partitionKey).build());
        item.put(SORT_KEY, AttributeValue.builder().n(sortKey).build());
        item.put("string_payload", AttributeValue.builder().s(payload).build());
        item.put("number_payload", AttributeValue.builder().n("1234").build());
        return item;
    }

    private static class TestingResultHandler implements ResultHandler<DynamoDbWriteRequest> {

        private boolean isComplete = false;
        private Exception exception;

        private List<DynamoDbWriteRequest> retryEntries = new ArrayList<>();

        @Override
        public void complete() {
            isComplete = true;
        }

        @Override
        public void completeExceptionally(Exception e) {
            exception = e;
        }

        @Override
        public void retryForEntries(List<DynamoDbWriteRequest> list) {
            retryEntries.addAll(list);
        }

        public boolean isComplete() {
            return isComplete;
        }

        public Exception getException() {
            return exception;
        }

        public List<DynamoDbWriteRequest> getRetryEntries() {
            return retryEntries;
        }
    }

    private static class TestAsyncDynamoDbClientProvider
            implements SdkClientProvider<DynamoDbAsyncClient> {

        private final DynamoDbAsyncClient dynamoDbAsyncClient;
        private int closeCount = 0;

        private TestAsyncDynamoDbClientProvider(DynamoDbAsyncClient dynamoDbAsyncClient) {
            this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        }

        @Override
        public DynamoDbAsyncClient getClient() {
            return dynamoDbAsyncClient;
        }

        @Override
        public void close() {
            closeCount++;
        }

        public int getCloseCount() {
            return closeCount;
        }
    }

    private static class TrackingDynamoDbAsyncClient implements DynamoDbAsyncClient {

        private List<List<WriteRequest>> requestHistory = new ArrayList<>();

        @Override
        public String serviceName() {
            return "DynamoDB";
        }

        @Override
        public void close() {}

        @Override
        public CompletableFuture<BatchWriteItemResponse> batchWriteItem(
                BatchWriteItemRequest batchWriteItemRequest) {
            requestHistory.add(batchWriteItemRequest.requestItems().get(TABLE_NAME));
            return CompletableFuture.completedFuture(BatchWriteItemResponse.builder().build());
        }

        @Override
        public DynamoDbServiceClientConfiguration serviceClientConfiguration() {
            return DynamoDbServiceClientConfiguration.builder().build();
        }

        public List<List<WriteRequest>> getRequestHistory() {
            return requestHistory;
        }
    }

    private static class ThrowingDynamoDbAsyncClient<T extends Throwable>
            implements DynamoDbAsyncClient {

        private final Optional<T> errorToReturn;
        private final Predicate<String> failWhenMatched;

        private ThrowingDynamoDbAsyncClient(
                Optional<T> errorToReturn, Predicate<String> failWhenMatched) {
            this.errorToReturn = errorToReturn;
            this.failWhenMatched = failWhenMatched;
        }

        @Override
        public String serviceName() {
            return "DynamoDB";
        }

        @Override
        public void close() {}

        @Override
        public CompletableFuture<BatchWriteItemResponse> batchWriteItem(
                BatchWriteItemRequest batchWriteItemRequest) {
            if (errorToReturn.isPresent()) {
                CompletableFuture<BatchWriteItemResponse> future = new CompletableFuture<>();
                future.completeExceptionally(
                        DynamoDbException.builder().cause((errorToReturn.get())).build());
                return future;
            }

            List<WriteRequest> failedRequests =
                    batchWriteItemRequest.requestItems().get(TABLE_NAME).stream()
                            .filter(
                                    writeRequest ->
                                            failWhenMatched.test(
                                                    writeRequest
                                                            .putRequest()
                                                            .item()
                                                            .get(PARTITION_KEY)
                                                            .s()))
                            .collect(toList());

            BatchWriteItemResponse.Builder responseBuilder = BatchWriteItemResponse.builder();
            if (!failedRequests.isEmpty()) {
                responseBuilder =
                        responseBuilder.unprocessedItems(singletonMap(TABLE_NAME, failedRequests));
            }
            return CompletableFuture.completedFuture(responseBuilder.build());
        }

        @Override
        public DynamoDbServiceClientConfiguration serviceClientConfiguration() {
            return DynamoDbServiceClientConfiguration.builder().build();
        }
    }

    private static class FailingRecordsDynamoDbAsyncClient implements DynamoDbAsyncClient {
        private final Predicate<String> failWhenMatched;

        private FailingRecordsDynamoDbAsyncClient(Predicate<String> failWhenMatched) {
            this.failWhenMatched = failWhenMatched;
        }

        @Override
        public String serviceName() {
            return "DynamoDB";
        }

        @Override
        public void close() {}

        @Override
        public CompletableFuture<BatchWriteItemResponse> batchWriteItem(
                BatchWriteItemRequest batchWriteItemRequest) {
            List<WriteRequest> failedRequests =
                    batchWriteItemRequest.requestItems().get(TABLE_NAME).stream()
                            .filter(
                                    writeRequest -> {
                                        if (writeRequest.putRequest() != null) {
                                            return failWhenMatched.test(
                                                    writeRequest
                                                            .putRequest()
                                                            .item()
                                                            .get(PARTITION_KEY)
                                                            .s());
                                        } else if (writeRequest.deleteRequest() != null) {
                                            return failWhenMatched.test(
                                                    writeRequest
                                                            .deleteRequest()
                                                            .key()
                                                            .get(PARTITION_KEY)
                                                            .s());
                                        } else {
                                            throw new RuntimeException(
                                                    "Write request cannot be empty");
                                        }
                                    })
                            .collect(toList());

            BatchWriteItemResponse.Builder responseBuilder = BatchWriteItemResponse.builder();
            if (!failedRequests.isEmpty()) {
                responseBuilder =
                        responseBuilder.unprocessedItems(singletonMap(TABLE_NAME, failedRequests));
            }
            return CompletableFuture.completedFuture(responseBuilder.build());
        }

        @Override
        public DynamoDbServiceClientConfiguration serviceClientConfiguration() {
            return DynamoDbServiceClientConfiguration.builder().build();
        }
    }
}
