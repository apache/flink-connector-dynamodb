package org.apache.flink.connector.kinesis.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.testutils.LocalstackContainer;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.amazonaws.kinesis.agg.AggRecord;
import com.amazonaws.kinesis.agg.RecordAggregator;
import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.rnorth.ducttape.ratelimits.RateLimiter;
import org.rnorth.ducttape.ratelimits.RateLimiterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;
import software.amazon.awssdk.services.kinesis.model.ScalingType;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountRequest;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.HTTP_PROTOCOL_VERSION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.TRUST_ALL_CERTIFICATES;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.InitialPosition;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.STREAM_INITIAL_POSITION;

/**
 * IT cases for using {@code KinesisStreamsSource} using a localstack container. StopWithSavepoint
 * IT cases have not been added because it is not exposed in the new source API.
 */
@Testcontainers
@ExtendWith(MiniClusterExtension.class)
public class KinesisStreamsSourceITCase {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisStreamsSourceITCase.class);
    private static final String LOCALSTACK_DOCKER_IMAGE_VERSION = "localstack/localstack:3.7.2";

    @Container
    private static final LocalstackContainer MOCK_KINESIS_CONTAINER =
            new LocalstackContainer(DockerImageName.parse(LOCALSTACK_DOCKER_IMAGE_VERSION));

    private StreamExecutionEnvironment env;
    private SdkHttpClient httpClient;
    private KinesisClient kinesisClient;

    @BeforeEach
    void setUp() {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        httpClient = AWSServicesTestUtils.createHttpClient();
        kinesisClient =
                AWSServicesTestUtils.createAwsSyncClient(
                        MOCK_KINESIS_CONTAINER.getEndpoint(), httpClient, KinesisClient.builder());
    }

    @AfterEach
    void teardown() {
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
        AWSGeneralUtil.closeResources(httpClient, kinesisClient);
    }

    @Test
    void nonExistentStreamShouldResultInFailure() {
        Assertions.assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(
                        () ->
                                new Scenario()
                                        .localstackStreamName("stream-exists")
                                        .withSourceConnectionStreamArn(
                                                "arn:aws:kinesis:ap-southeast-1:000000000000:stream/stream-not-exists")
                                        .runScenario())
                .withStackTraceContaining(
                        "Stream arn arn:aws:kinesis:ap-southeast-1:000000000000:stream/stream-not-exists not found");
    }

    @Test
    void singleShardStreamIsConsumed() throws Exception {
        new Scenario()
                .localstackStreamName("single-shard-stream")
                .shardCount(1)
                .withSourceConnectionStreamArn(
                        "arn:aws:kinesis:ap-southeast-1:000000000000:stream/single-shard-stream")
                .runScenario();
    }

    @Test
    void singleShardStreamWithAggregationIsConsumed() throws Exception {
        new Scenario()
                .localstackStreamName("single-shard-stream-aggregation")
                .shardCount(1)
                .aggregationFactor(10)
                .withSourceConnectionStreamArn(
                        "arn:aws:kinesis:ap-southeast-1:000000000000:stream/single-shard-stream-aggregation")
                .runScenario();
    }

    @Test
    void multipleShardStreamIsConsumed() throws Exception {
        new Scenario()
                .localstackStreamName("multiple-shard-stream")
                .shardCount(4)
                .withSourceConnectionStreamArn(
                        "arn:aws:kinesis:ap-southeast-1:000000000000:stream/multiple-shard-stream")
                .runScenario();
    }

    @Test
    void multipleShardStreamWithAggregationIsConsumed() throws Exception {
        new Scenario()
                .localstackStreamName("multiple-shard-stream-aggregation")
                .shardCount(4)
                .aggregationFactor(10)
                .withSourceConnectionStreamArn(
                        "arn:aws:kinesis:ap-southeast-1:000000000000:stream/multiple-shard-stream-aggregation")
                .runScenario();
    }

    @Test
    void reshardedStreamIsConsumed() throws Exception {
        new Scenario()
                .localstackStreamName("resharded-stream")
                .shardCount(1)
                .reshardStream(2)
                .withSourceConnectionStreamArn(
                        "arn:aws:kinesis:ap-southeast-1:000000000000:stream/resharded-stream")
                .runScenario();
    }

    @Test
    void reshardedStreamWithAggregationIsConsumed() throws Exception {
        new Scenario()
                .localstackStreamName("resharded-stream-aggregation")
                .shardCount(1)
                .aggregationFactor(10)
                .reshardStream(2)
                .withSourceConnectionStreamArn(
                        "arn:aws:kinesis:ap-southeast-1:000000000000:stream/resharded-stream-aggregation")
                .runScenario();
    }

    private Configuration getDefaultConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setString(AWS_ENDPOINT, MOCK_KINESIS_CONTAINER.getEndpoint());
        configuration.setString(AWS_ACCESS_KEY_ID, "accessKeyId");
        configuration.setString(AWS_SECRET_ACCESS_KEY, "secretAccessKey");
        configuration.setString(AWS_REGION, Region.AP_SOUTHEAST_1.toString());
        configuration.setString(TRUST_ALL_CERTIFICATES, "true");
        configuration.setString(HTTP_PROTOCOL_VERSION, "HTTP1_1");
        configuration.set(STREAM_INITIAL_POSITION, InitialPosition.TRIM_HORIZON);
        return configuration;
    }

    private class Scenario {
        private final int expectedElements = 1000;
        private int aggregationFactor = 1;
        private String localstackStreamName = null;
        private int shardCount = 1;
        private boolean shouldReshardStream = false;
        private int targetReshardCount = -1;
        private String sourceConnectionStreamArn;
        private final Configuration configuration =
                KinesisStreamsSourceITCase.this.getDefaultConfiguration();

        public void runScenario() throws Exception {
            if (localstackStreamName != null) {
                prepareStream(localstackStreamName);
            }

            putRecordsWithReshard(localstackStreamName, expectedElements);

            KinesisStreamsSource<String> kdsSource =
                    KinesisStreamsSource.<String>builder()
                            .setStreamArn(sourceConnectionStreamArn)
                            .setSourceConfig(configuration)
                            .setDeserializationSchema(new SimpleStringSchema())
                            .build();

            List<String> result =
                    env.fromSource(kdsSource, WatermarkStrategy.noWatermarks(), "Kinesis source")
                            .returns(TypeInformation.of(String.class))
                            .executeAndCollect(expectedElements);

            Assertions.assertThat(result.size()).isEqualTo(expectedElements);
        }

        public Scenario withSourceConnectionStreamArn(String sourceConnectionStreamArn) {
            this.sourceConnectionStreamArn = sourceConnectionStreamArn;
            return this;
        }

        public Scenario localstackStreamName(String localstackStreamName) {
            this.localstackStreamName = localstackStreamName;
            return this;
        }

        public Scenario shardCount(int shardCount) {
            this.shardCount = shardCount;
            return this;
        }

        public Scenario reshardStream(int targetShardCount) {
            this.shouldReshardStream = true;
            this.targetReshardCount = targetShardCount;
            return this;
        }

        public Scenario aggregationFactor(int aggregationFactor) {
            this.aggregationFactor = aggregationFactor;
            return this;
        }

        private void prepareStream(String streamName) throws Exception {
            final RateLimiter rateLimiter =
                    RateLimiterBuilder.newBuilder()
                            .withRate(1, SECONDS)
                            .withConstantThroughput()
                            .build();

            kinesisClient.createStream(
                    CreateStreamRequest.builder()
                            .streamName(streamName)
                            .shardCount(shardCount)
                            .build());

            Deadline deadline = Deadline.fromNow(Duration.ofMinutes(1));
            while (!rateLimiter.getWhenReady(() -> streamExists(streamName))) {
                if (deadline.isOverdue()) {
                    throw new RuntimeException("Failed to create stream within time");
                }
            }
        }

        private void putRecordsWithReshard(String name, int numRecords) {
            int midpoint = numRecords / 2;
            putRecords(name, 0, midpoint);
            if (shouldReshardStream) {
                reshard(name);
            }
            putRecords(name, midpoint, numRecords);
        }

        private void putRecords(String streamName, int startInclusive, int endInclusive) {
            List<byte[]> messages =
                    IntStream.range(startInclusive, endInclusive)
                            .mapToObj(String::valueOf)
                            .map(String::getBytes)
                            .collect(Collectors.toList());

            for (List<byte[]> partition : Lists.partition(messages, 500)) {
                List<PutRecordsRequestEntry> entries =
                        Lists.partition(partition, aggregationFactor).stream()
                                .map(this::createAggregatePutRecordsRequestEntry)
                                .collect(Collectors.toList());
                PutRecordsRequest requests =
                        PutRecordsRequest.builder().streamName(streamName).records(entries).build();
                PutRecordsResponse putRecordResult = kinesisClient.putRecords(requests);
                for (PutRecordsResultEntry result : putRecordResult.records()) {
                    LOG.debug("Added record: {}", result.sequenceNumber());
                }
            }
        }

        private PutRecordsRequestEntry createAggregatePutRecordsRequestEntry(
                List<byte[]> messages) {
            RecordAggregator recordAggregator = new RecordAggregator();

            for (byte[] message : messages) {
                try {
                    recordAggregator.addUserRecord("key", message);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to add record to aggregator", e);
                }
            }

            AggRecord aggRecord = recordAggregator.clearAndGet();
            return aggRecord.toPutRecordsRequestEntry();
        }

        private void reshard(String streamName) {
            kinesisClient.updateShardCount(
                    UpdateShardCountRequest.builder()
                            .streamName(streamName)
                            .targetShardCount(targetReshardCount)
                            .scalingType(ScalingType.UNIFORM_SCALING)
                            .build());
        }

        private boolean streamExists(final String streamName) {
            try {
                return kinesisClient
                                .describeStream(
                                        DescribeStreamRequest.builder()
                                                .streamName(streamName)
                                                .build())
                                .streamDescription()
                                .streamStatus()
                        == StreamStatus.ACTIVE;
            } catch (Exception e) {
                return false;
            }
        }
    }
}
