package org.apache.flink.connector.kinesis.source.util;

import org.apache.flink.connector.kinesis.source.proxy.AsyncStreamProxy;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.Lists.partition;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.kinesis.source.split.StartingPositionUtil.toSdkStartingPosition;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Factory for different kinds of fake Kinesis behaviours using the {@link AsyncStreamProxy}
 * interface.
 */
public class FakeKinesisFanOutBehaviorsFactory {

    // ------------------------------------------------------------------------
    //  Behaviours related to subscribe to shard and consuming data
    // ------------------------------------------------------------------------
    public static SingleShardFanOutStreamProxy.Builder boundedShard() {
        return new SingleShardFanOutStreamProxy.Builder();
    }

    public static AsyncStreamProxy resourceNotFoundWhenObtainingSubscription() {
        return new ExceptionalFanOutStreamProxy(ResourceNotFoundException.builder().build());
    }

    public static TrackCloseStreamProxy testCloseStreamProxy() {
        return new TrackCloseStreamProxy();
    }

    /** Test Stream Proxy to closure of StreamProxy. */
    public static class TrackCloseStreamProxy implements AsyncStreamProxy {

        public boolean isClosed() {
            return closed;
        }

        private boolean closed = false;

        @Override
        public CompletableFuture<Void> subscribeToShard(
                String consumerArn,
                String shardId,
                org.apache.flink.connector.kinesis.source.split.StartingPosition startingPosition,
                SubscribeToShardResponseHandler responseHandler) {
            // no-op
            throw new UnsupportedOperationException(
                    "This method is not supported on the TrackCloseStreamProxy.");
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }

    /**
     * A fake implementation of KinesisProxyV2 SubscribeToShard that provides dummy records for EFO
     * subscriptions. Aggregated and non-aggregated records are supported with various batch and
     * subscription sizes.
     */
    public static class SingleShardFanOutStreamProxy extends AbstractSingleShardFanOutStreamProxy {

        private final int batchesPerSubscription;

        private final int recordsPerBatch;

        private final long millisBehindLatest;

        private final int totalRecords;

        private final int aggregationFactor;

        private final AtomicInteger sequenceNumber = new AtomicInteger();

        private SingleShardFanOutStreamProxy(final Builder builder) {
            super(builder.getSubscriptionCount());
            this.batchesPerSubscription = builder.batchesPerSubscription;
            this.recordsPerBatch = builder.recordsPerBatch;
            this.millisBehindLatest = builder.millisBehindLatest;
            this.aggregationFactor = builder.aggregationFactor;
            this.totalRecords = builder.getTotalRecords();
        }

        @Override
        List<SubscribeToShardEvent> getEventsToSend() {
            List<SubscribeToShardEvent> events = new ArrayList<>();

            SubscribeToShardEvent.Builder eventBuilder =
                    SubscribeToShardEvent.builder().millisBehindLatest(millisBehindLatest);

            for (int batchIndex = 0;
                    batchIndex < batchesPerSubscription && sequenceNumber.get() < totalRecords;
                    batchIndex++) {
                List<Record> records = new ArrayList<>();

                for (int i = 0; i < recordsPerBatch; i++) {
                    records.add(createRecord(sequenceNumber));
                }

                List<Record> aggregatedRecords =
                        partition(records, aggregationFactor).stream()
                                .map(TestUtil::createAggregatedRecord)
                                .collect(Collectors.toList());

                eventBuilder.records(aggregatedRecords);

                String continuation =
                        sequenceNumber.get() < totalRecords
                                ? String.valueOf(sequenceNumber.get() + 1)
                                : null;
                eventBuilder.continuationSequenceNumber(continuation);

                events.add(eventBuilder.build());
            }

            return events;
        }

        /** A convenience builder for {@link SingleShardFanOutStreamProxy}. */
        public static class Builder {
            private int batchesPerSubscription = 100000;
            private int recordsPerBatch = 10;
            private long millisBehindLatest = 0;
            private int batchCount = 1;
            private int aggregationFactor = 1;

            public int getSubscriptionCount() {
                return (int)
                        Math.ceil(
                                (double) getTotalRecords()
                                        / batchesPerSubscription
                                        / recordsPerBatch);
            }

            public int getTotalRecords() {
                return batchCount * recordsPerBatch;
            }

            public Builder withBatchesPerSubscription(final int batchesPerSubscription) {
                this.batchesPerSubscription = batchesPerSubscription;
                return this;
            }

            public Builder withRecordsPerBatch(final int recordsPerBatch) {
                this.recordsPerBatch = recordsPerBatch;
                return this;
            }

            public Builder withBatchCount(final int batchCount) {
                this.batchCount = batchCount;
                return this;
            }

            public Builder withMillisBehindLatest(final long millisBehindLatest) {
                this.millisBehindLatest = millisBehindLatest;
                return this;
            }

            public Builder withAggregationFactor(final int aggregationFactor) {
                this.aggregationFactor = aggregationFactor;
                return this;
            }

            public SingleShardFanOutStreamProxy build() {
                return new SingleShardFanOutStreamProxy(this);
            }
        }
    }

    private static final class ExceptionalFanOutStreamProxy implements AsyncStreamProxy {

        private final RuntimeException exception;

        private ExceptionalFanOutStreamProxy(RuntimeException exception) {
            this.exception = exception;
        }

        @Override
        public CompletableFuture<Void> subscribeToShard(
                String consumerArn,
                String shardId,
                org.apache.flink.connector.kinesis.source.split.StartingPosition startingPosition,
                SubscribeToShardResponseHandler responseHandler) {
            responseHandler.exceptionOccurred(exception);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() throws IOException {
            // no-op
        }
    }

    /**
     * A single shard dummy EFO implementation that provides basic responses and subscription
     * management. Does not provide any records.
     */
    public abstract static class AbstractSingleShardFanOutStreamProxy implements AsyncStreamProxy {

        private final List<SubscribeToShardRequest> requests = new ArrayList<>();
        private int remainingSubscriptions;

        private AbstractSingleShardFanOutStreamProxy(final int remainingSubscriptions) {
            this.remainingSubscriptions = remainingSubscriptions;
        }

        public int getNumberOfSubscribeToShardInvocations() {
            return requests.size();
        }

        public StartingPosition getStartingPositionForSubscription(final int subscriptionIndex) {
            assertThat(subscriptionIndex).isGreaterThanOrEqualTo(0);
            assertThat(subscriptionIndex).isLessThan(getNumberOfSubscribeToShardInvocations());

            return requests.get(subscriptionIndex).startingPosition();
        }

        @Override
        public CompletableFuture<Void> subscribeToShard(
                final String consumerArn,
                final String shardId,
                final org.apache.flink.connector.kinesis.source.split.StartingPosition
                        startingPosition,
                final SubscribeToShardResponseHandler responseHandler) {

            requests.add(
                    SubscribeToShardRequest.builder()
                            .consumerARN(consumerArn)
                            .shardId(shardId)
                            .startingPosition(toSdkStartingPosition(startingPosition))
                            .build());

            return CompletableFuture.supplyAsync(
                    () -> {
                        responseHandler.responseReceived(
                                SubscribeToShardResponse.builder().build());
                        responseHandler.onEventStream(
                                subscriber -> {
                                    final List<SubscribeToShardEvent> eventsToSend;

                                    if (remainingSubscriptions > 0) {
                                        eventsToSend = getEventsToSend();
                                        remainingSubscriptions--;
                                    } else {
                                        eventsToSend =
                                                Collections.singletonList(
                                                        SubscribeToShardEvent.builder()
                                                                .millisBehindLatest(0L)
                                                                .continuationSequenceNumber(null)
                                                                .build());
                                    }

                                    Iterator<SubscribeToShardEvent> iterator =
                                            eventsToSend.iterator();

                                    Subscription subscription =
                                            new FakeSubscription(
                                                    (n) -> {
                                                        if (iterator.hasNext()) {
                                                            subscriber.onNext(iterator.next());
                                                        } else {
                                                            completeSubscription(subscriber);
                                                        }
                                                    });
                                    subscriber.onSubscribe(subscription);
                                });
                        return null;
                    });
        }

        void completeSubscription(Subscriber<? super SubscribeToShardEventStream> subscriber) {
            subscriber.onComplete();
        }

        abstract List<SubscribeToShardEvent> getEventsToSend();

        @Override
        public void close() throws IOException {
            // no-op
        }
    }

    private static class FakeSubscription implements Subscription {

        private final java.util.function.Consumer<Long> onRequest;

        public FakeSubscription(java.util.function.Consumer<Long> onRequest) {
            this.onRequest = onRequest;
        }

        @Override
        public void request(long n) {
            onRequest.accept(n);
        }

        @Override
        public void cancel() {
            // Nothing to do
        }
    }

    private static Record createRecord(final AtomicInteger sequenceNumber) {
        return createRecord(randomAlphabetic(32).getBytes(UTF_8), sequenceNumber);
    }

    private static Record createRecord(final byte[] data, final AtomicInteger sequenceNumber) {
        return Record.builder()
                .approximateArrivalTimestamp(Instant.now())
                .data(SdkBytes.fromByteArray(data))
                .sequenceNumber(String.valueOf(sequenceNumber.incrementAndGet()))
                .partitionKey("pk")
                .build();
    }

    private static List<SubscribeToShardEvent> generateEvents(
            int numberOfEvents, AtomicInteger sequenceNumber) {
        return IntStream.range(0, numberOfEvents)
                .mapToObj(
                        i ->
                                SubscribeToShardEvent.builder()
                                        .records(createRecord(sequenceNumber))
                                        .continuationSequenceNumber(String.valueOf(i))
                                        .build())
                .collect(Collectors.toList());
    }
}
