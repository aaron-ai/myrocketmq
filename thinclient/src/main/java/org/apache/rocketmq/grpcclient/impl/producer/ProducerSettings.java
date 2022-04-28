package org.apache.rocketmq.grpcclient.impl.producer;

import apache.rocketmq.v2.CustomizedBackoff;
import apache.rocketmq.v2.ExponentialBackoff;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.RetryPolicy;
import apache.rocketmq.v2.Settings;
import com.google.protobuf.util.Durations;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.rocketmq.apis.retry.ExponentialBackoffRetryPolicy;
import org.apache.rocketmq.grpcclient.impl.ClientSettings;
import org.apache.rocketmq.grpcclient.impl.ClientType;
import org.apache.rocketmq.grpcclient.impl.CustomizedBackoffRetryPolicy;
import org.apache.rocketmq.grpcclient.message.protocol.Resource;
import org.apache.rocketmq.grpcclient.route.Endpoints;

public class ProducerSettings extends ClientSettings {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerSettings.class);

    private final Set<Resource> topics;
    private volatile int compressBodyThresholdBytes = 4 * 1024;
    private volatile int maxBodySizeBytes = 4 * 1024 * 1024;

    public ProducerSettings(String clientId, Endpoints accessPoint,
        ExponentialBackoffRetryPolicy exponentialBackoffRetryPolicy,
        Duration requestTimeout, Set<Resource> topics) {
        super(clientId, ClientType.PRODUCER, accessPoint, exponentialBackoffRetryPolicy, requestTimeout);
        this.topics = topics;
    }

    public int getCompressBodyThresholdBytes() {
        return compressBodyThresholdBytes;
    }

    public int getMaxBodySizeBytes() {
        return maxBodySizeBytes;
    }

    @Override
    public Settings toProtobuf() {
        final Publishing publishing = Publishing.newBuilder().addAllTopics(topics.stream().map(Resource::toProtobuf).collect(Collectors.toList())).build();
        final Settings.Builder builder = Settings.newBuilder().setAccessPoint(accessPoint.toProtobuf()).setClientType(clientType.toProtobuf())
            .setRequestTimeout(Durations.fromNanos(requestTimeout.toNanos())).setPublishing(publishing);
        if (retryPolicy instanceof ExponentialBackoffRetryPolicy) {
            ExponentialBackoffRetryPolicy exponentialBackoffRetryPolicy = (ExponentialBackoffRetryPolicy) retryPolicy;
            ExponentialBackoff exponentialBackoff = ExponentialBackoff.newBuilder()
                .setMultiplier((float) exponentialBackoffRetryPolicy.getBackoffMultiplier())
                .setMax(Durations.fromNanos(exponentialBackoffRetryPolicy.getMaxBackoff().toNanos()))
                .setInitial(Durations.fromNanos(exponentialBackoffRetryPolicy.getInitialBackoff().toNanos()))
                .build();
            RetryPolicy retryPolicy = RetryPolicy.newBuilder()
                .setMaxAttempts(exponentialBackoffRetryPolicy.getMaxAttempts())
                .setExponentialBackoff(exponentialBackoff)
                .build();
            builder.setBackoffPolicy(retryPolicy);
        }
        // Actually never reach here.
        if (retryPolicy instanceof CustomizedBackoffRetryPolicy) {
            CustomizedBackoffRetryPolicy customizedBackoffRetryPolicy = (CustomizedBackoffRetryPolicy) retryPolicy;
            CustomizedBackoff customizedBackoff = CustomizedBackoff.newBuilder()
                .addAllNext(customizedBackoffRetryPolicy.getDurations().stream().map(duration -> Durations.fromNanos(duration.toNanos())).collect(Collectors.toList()))
                .build();
            RetryPolicy retryPolicy = RetryPolicy.newBuilder()
                .setMaxAttempts(customizedBackoffRetryPolicy.getMaxAttempts())
                .setCustomizedBackoff(customizedBackoff)
                .build();
            builder.setBackoffPolicy(retryPolicy);
        }
        return builder.build();
    }

    @Override
    public void applySettings(Settings settings) {
        final Settings.PubSubCase pubSubCase = settings.getPubSubCase();
        if (!Settings.PubSubCase.PUBLISHING.equals(pubSubCase)) {
            LOGGER.error("[Bug] Issued settings not match with the client type, client id ={}, pub-sub case={}, client type={}", clientId, pubSubCase, clientType);
            return;
        }
        final Publishing publishing = settings.getPublishing();
        this.compressBodyThresholdBytes = publishing.getCompressBodyThreshold();
        this.maxBodySizeBytes = publishing.getMaxBodySize();
        this.firstApplyCompletedFuture.set(null);
    }
}
