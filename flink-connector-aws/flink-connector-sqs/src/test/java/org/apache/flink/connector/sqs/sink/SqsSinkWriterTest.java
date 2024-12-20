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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.connector.sqs.sink.client.SdkClientProvider;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.BatchRequestTooLongException;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.ResourceNotFoundException;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Covers construction, defaults and sanity checking of {@link SqsSinkWriter}. */
public class SqsSinkWriterTest {

    private SqsSinkWriter<String> sinkWriter;

    private static final ElementConverter<String, SendMessageBatchRequestEntry>
            ELEMENT_CONVERTER_PLACEHOLDER =
                    SqsSinkElementConverter.<String>builder()
                            .setSerializationSchema(new SimpleStringSchema())
                            .build();

    @Test
    public void testNonRetryableExceptionWhenFailOnErrorFalseWillNotRetry() throws IOException {
        Optional<Exception> exceptionToThrow = getGenericNonRetryableException();
        ThrowingSqsAsyncClient sqsAsyncClient = new ThrowingSqsAsyncClient(exceptionToThrow);
        TestSqsAsyncClientProvider testSqsAsyncClientProvider =
                new TestSqsAsyncClientProvider(sqsAsyncClient);
        sinkWriter = getSqsSinkWriter(false, testSqsAsyncClientProvider);
        TestingResultHandler resultHandler = new TestingResultHandler();
        sinkWriter.submitRequestEntries(getDefaultInputRequests(), resultHandler);
        assertThat(resultHandler.isComplete()).isFalse();
        assertThat(resultHandler.getExceptionThrown()).isNotNull();
    }

    @Test
    public void testRetryableExceptionWhenFailOnErrorTrueWillNotRetry() throws IOException {
        Optional<Exception> exceptionToThrow = getGenericRetryableException();
        ThrowingSqsAsyncClient sqsAsyncClient = new ThrowingSqsAsyncClient(exceptionToThrow);
        TestSqsAsyncClientProvider testSqsAsyncClientProvider =
                new TestSqsAsyncClientProvider(sqsAsyncClient);
        sinkWriter = getSqsSinkWriter(true, testSqsAsyncClientProvider);

        TestingResultHandler resultHandler = new TestingResultHandler();
        sinkWriter.submitRequestEntries(getDefaultInputRequests(), resultHandler);
        assertThat(resultHandler.isComplete()).isFalse();
        assertThat(resultHandler.getExceptionThrown()).isNotNull();
    }

    @Test
    public void testRetryableExceptionWhenFailOnErrorFalseWillRetry() throws IOException {
        Optional<Exception> exceptionToThrow = getGenericRetryableException();
        ThrowingSqsAsyncClient sqsAsyncClient = new ThrowingSqsAsyncClient(exceptionToThrow);
        TestSqsAsyncClientProvider testSqsAsyncClientProvider =
                new TestSqsAsyncClientProvider(sqsAsyncClient);
        sinkWriter = getSqsSinkWriter(false, testSqsAsyncClientProvider);

        TestingResultHandler resultHandler = new TestingResultHandler();
        sinkWriter.submitRequestEntries(getDefaultInputRequests(), resultHandler);
        assertThat(resultHandler.isComplete()).isFalse();
        assertThat(resultHandler.getFailedRequests()).hasSize(2);
    }

    @Test
    public void testSubmitRequestEntriesWithNoException() throws IOException {
        TrackingSqsAsyncClient sqsAsyncClient = new TrackingSqsAsyncClient();
        TestSqsAsyncClientProvider testSqsAsyncClientProvider =
                new TestSqsAsyncClientProvider(sqsAsyncClient);
        sinkWriter = getSqsSinkWriter(false, testSqsAsyncClientProvider);

        TestingResultHandler resultHandler = new TestingResultHandler();

        sinkWriter.submitRequestEntries(getDefaultInputRequests(), resultHandler);

        assertThat(resultHandler.isComplete()).isTrue();
        assertThat(resultHandler.getFailedRequests()).isEmpty();
    }

    @Test
    public void testSubmitRequestEntriesWithPartialSuccessWithFailOnErrorFalseWillRetry()
            throws IOException {
        FailedSqsAsyncClient sqsAsyncClient = new FailedSqsAsyncClient("testId2");
        TestSqsAsyncClientProvider testSqsAsyncClientProvider =
                new TestSqsAsyncClientProvider(sqsAsyncClient);
        sinkWriter = getSqsSinkWriter(false, testSqsAsyncClientProvider);

        TestingResultHandler resultHandler = new TestingResultHandler();
        sinkWriter.submitRequestEntries(getDefaultInputRequests(), resultHandler);

        assertThat(resultHandler.getFailedRequests()).hasSize(1);
    }

    @Test
    public void
            testSubmitRequestEntriesWithPartialSuccessWithFailOnErrorFalseAndFailedIdDoesNotMatchWillNotRetry()
                    throws IOException {
        FailedSqsAsyncClient sqsAsyncClient = new FailedSqsAsyncClient("invalidFailedRequestId");
        TestSqsAsyncClientProvider testSqsAsyncClientProvider =
                new TestSqsAsyncClientProvider(sqsAsyncClient);
        sinkWriter = getSqsSinkWriter(false, testSqsAsyncClientProvider);

        TestingResultHandler resultHandler = new TestingResultHandler();

        sinkWriter.submitRequestEntries(getDefaultInputRequests(), resultHandler);
        assertThat(resultHandler.isComplete()).isFalse();
        assertThat(resultHandler.getExceptionThrown()).isNotNull();
    }

    @Test
    public void testSubmitRequestEntriesWithPartialSuccessWithFailOnErrorTrueWillNotRetry()
            throws IOException {

        FailedSqsAsyncClient sqsAsyncClient = new FailedSqsAsyncClient("testId2");
        TestSqsAsyncClientProvider testSqsAsyncClientProvider =
                new TestSqsAsyncClientProvider(sqsAsyncClient);
        sinkWriter = getSqsSinkWriter(true, testSqsAsyncClientProvider);

        TestingResultHandler resultHandler = new TestingResultHandler();

        sinkWriter.submitRequestEntries(getDefaultInputRequests(), resultHandler);
        assertThat(resultHandler.isComplete()).isFalse();
        assertThat(resultHandler.getExceptionThrown()).isNotNull();
    }

    @Test
    public void testClientClosesWhenWriterIsClosed() throws IOException {
        TrackingSqsAsyncClient sqsAsyncClient = new TrackingSqsAsyncClient();
        TestSqsAsyncClientProvider testSqsAsyncClientProvider =
                new TestSqsAsyncClientProvider(sqsAsyncClient);
        sinkWriter = getSqsSinkWriter(false, testSqsAsyncClientProvider);
        sinkWriter.close();
        assertThat(testSqsAsyncClientProvider.getCloseCount()).isEqualTo(1);
    }

    @Test
    void testGetSizeInBytesReturnsSizeOfBlob() throws IOException {
        TrackingSqsAsyncClient sqsAsyncClient = new TrackingSqsAsyncClient();
        TestSqsAsyncClientProvider testSqsAsyncClientProvider =
                new TestSqsAsyncClientProvider(sqsAsyncClient);
        sinkWriter = getSqsSinkWriter(false, testSqsAsyncClientProvider);

        String testString = "{many hands make light work;";
        SendMessageBatchRequestEntry record =
                SendMessageBatchRequestEntry.builder().messageBody(testString).build();
        assertThat(sinkWriter.getSizeInBytes(record))
                .isEqualTo(testString.getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    void getNumRecordsOutErrorsCounterRecordsCorrectNumberOfFailures()
            throws IOException, InterruptedException {
        TestSinkInitContext ctx = new TestSinkInitContext();
        SqsSink<String> sqsSink =
                new SqsSink<>(
                        ELEMENT_CONVERTER_PLACEHOLDER,
                        10,
                        16,
                        10000,
                        4 * 1024 * 1024L,
                        5000L,
                        1000 * 1024L,
                        true,
                        "test-stream",
                        AWSServicesTestUtils.createConfig("https://localhost"));
        SinkWriter<String> writer = sqsSink.createWriter(ctx);

        for (int i = 0; i < 12; i++) {
            writer.write("data_bytes", null);
        }
        assertThatExceptionOfType(CompletionException.class)
                .isThrownBy(() -> writer.flush(true))
                .withCauseInstanceOf(SdkClientException.class)
                .withMessageContaining(
                        "Unable to execute HTTP request: Connection refused: localhost/127.0.0.1:443");
        assertThat(ctx.metricGroup().getNumRecordsOutErrorsCounter().getCount()).isEqualTo(10);
        assertThat(ctx.metricGroup().getNumRecordsSendErrorsCounter().getCount()).isEqualTo(10);
    }

    private SqsSinkWriter<String> getSqsSinkWriter(
            final boolean failOnError, TestSqsAsyncClientProvider testSqsAsyncClientProvider)
            throws IOException {
        TestSinkInitContext sinkInitContext = new TestSinkInitContext();
        Properties sinkProperties = AWSServicesTestUtils.createConfig("https://fake_aws_endpoint");
        SqsSink<String> sink =
                new SqsSink<>(
                        ELEMENT_CONVERTER_PLACEHOLDER,
                        10,
                        16,
                        10000,
                        4 * 1024 * 1024L,
                        5000L,
                        1000 * 1024L,
                        failOnError,
                        "https://sqs.us-east-2.amazonaws.com/618277569814/fake-sqs",
                        sinkProperties);
        sink.setSqsAsyncClientProvider(testSqsAsyncClientProvider);
        SqsSinkWriter sqsSinkWriter = (SqsSinkWriter<String>) sink.createWriter(sinkInitContext);
        return sqsSinkWriter;
    }

    private List<SendMessageBatchRequestEntry> getDefaultInputRequests() {
        return Arrays.asList(
                sinkSendMessageBatchRequestEntry("test1", "testId1"),
                sinkSendMessageBatchRequestEntry("test2", "testId2"));
    }

    private SendMessageBatchRequestEntry sinkSendMessageBatchRequestEntry(
            final String messageBody, final String id) {
        return SendMessageBatchRequestEntry.builder().id(id).messageBody(messageBody).build();
    }

    private Optional<Exception> getGenericRetryableException() {
        return Optional.of(
                BatchRequestTooLongException.builder()
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

    private static class TestingResultHandler
            implements ResultHandler<SendMessageBatchRequestEntry> {
        private boolean isComplete = false;
        private Exception exceptionThrown = null;
        private final List<SendMessageBatchRequestEntry> failedRequests = new ArrayList<>();

        @Override
        public void complete() {
            isComplete = true;
        }

        @Override
        public void completeExceptionally(Exception e) {
            exceptionThrown = e;
        }

        @Override
        public void retryForEntries(List<SendMessageBatchRequestEntry> list) {
            failedRequests.addAll(list);
        }

        public boolean isComplete() {
            return isComplete;
        }

        public Exception getExceptionThrown() {
            return exceptionThrown;
        }

        public List<SendMessageBatchRequestEntry> getFailedRequests() {
            return failedRequests;
        }
    }

    private static class ThrowingSqsAsyncClient<T extends Throwable> implements SqsAsyncClient {

        private final Optional<T> errorToReturn;

        private ThrowingSqsAsyncClient(Optional<T> errorToReturn) {
            this.errorToReturn = errorToReturn;
        }

        @Override
        public String serviceName() {
            return "SQS";
        }

        @Override
        public void close() {}

        @Override
        public CompletableFuture<SendMessageBatchResponse> sendMessageBatch(
                SendMessageBatchRequest sendMessageBatchRequest) {
            CompletableFuture<SendMessageBatchResponse> future = new CompletableFuture<>();
            future.completeExceptionally(
                    SqsException.builder().cause((errorToReturn.get())).build());
            return future;
        }
    }

    private static class TrackingSqsAsyncClient implements SqsAsyncClient {

        @Override
        public String serviceName() {
            return "SQS";
        }

        @Override
        public void close() {}

        @Override
        public CompletableFuture<SendMessageBatchResponse> sendMessageBatch(
                SendMessageBatchRequest sendMessageBatchRequest) {
            SendMessageBatchResultEntry sendMessageBatchResultEntry =
                    SendMessageBatchResultEntry.builder().build();
            List<SendMessageBatchResultEntry> sendMessageBatchResultEntryList = new ArrayList<>();
            sendMessageBatchResultEntryList.add(sendMessageBatchResultEntry);
            return CompletableFuture.completedFuture(
                    SendMessageBatchResponse.builder()
                            .failed(Collections.emptyList())
                            .successful(sendMessageBatchResultEntryList)
                            .build());
        }
    }

    private static class FailedSqsAsyncClient implements SqsAsyncClient {

        private final String failedRequestId;

        private FailedSqsAsyncClient(String failedRequestId) {
            this.failedRequestId = failedRequestId;
        }

        @Override
        public String serviceName() {
            return "SQS";
        }

        @Override
        public void close() {}

        @Override
        public CompletableFuture<SendMessageBatchResponse> sendMessageBatch(
                SendMessageBatchRequest sendMessageBatchRequest) {
            BatchResultErrorEntry batchResultErrorEntry =
                    BatchResultErrorEntry.builder().id(failedRequestId).build();
            List<BatchResultErrorEntry> batchResultErrorEntryList = new ArrayList<>();
            batchResultErrorEntryList.add(batchResultErrorEntry);

            SendMessageBatchResultEntry sendMessageBatchResultEntry =
                    SendMessageBatchResultEntry.builder().id("testId1").build();
            List<SendMessageBatchResultEntry> sendMessageBatchResultEntryList = new ArrayList<>();
            sendMessageBatchResultEntryList.add(sendMessageBatchResultEntry);
            return CompletableFuture.completedFuture(
                    SendMessageBatchResponse.builder()
                            .failed(batchResultErrorEntryList)
                            .successful(sendMessageBatchResultEntryList)
                            .build());
        }
    }

    private static class TestSqsAsyncClientProvider implements SdkClientProvider<SqsAsyncClient> {

        private final SqsAsyncClient sqsAsyncClient;
        private int closeCount = 0;

        private TestSqsAsyncClientProvider(SqsAsyncClient sqsAsyncClient) {
            this.sqsAsyncClient = sqsAsyncClient;
        }

        @Override
        public SqsAsyncClient getClient() {
            return sqsAsyncClient;
        }

        @Override
        public void close() {
            closeCount++;
        }

        public int getCloseCount() {
            return closeCount;
        }
    }
}
