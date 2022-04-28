package org.apache.rocketmq.grpcclient.impl.producer;

import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.Settings;
import com.google.protobuf.util.Durations;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.rocketmq.apis.retry.BackoffRetryPolicy;
import org.apache.rocketmq.grpcclient.impl.ClientSettings;
import org.apache.rocketmq.grpcclient.impl.ClientType;
import org.apache.rocketmq.grpcclient.message.protocol.Resource;
import org.apache.rocketmq.grpcclient.route.Endpoints;

public class ProducerSettings extends ClientSettings {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerSettings.class);

    private final List<Resource> topics;
    private volatile int compressBodyThresholdBytes = 4 * 1024;
    private volatile int maxBodySizeBytes = 4 * 1024 * 1024;

    public ProducerSettings(String clientId, Endpoints accessPoint, BackoffRetryPolicy backoffRetryPolicy,
        Duration requestTimeout, List<Resource> topics) {
        super(clientId, ClientType.PRODUCER, accessPoint, backoffRetryPolicy, requestTimeout);
        this.topics = topics;
    }

    public int getCompressBodyThresholdBytes() {
        return compressBodyThresholdBytes;
    }

    public int getMaxBodySizeBytes() {
        return maxBodySizeBytes;
    }

    // TODO: set backoff policy.
    @Override
    public Settings toProtobuf() {
        final Publishing publishing = Publishing.newBuilder().addAllTopics(topics.stream().map(Resource::toProtobuf).collect(Collectors.toList())).build();
        return Settings.newBuilder().setAccessPoint(accessPoint.toProtobuf()).setClientType(clientType.toProtobuf())
            .setRequestTimeout(Durations.fromNanos(requestTimeout.toNanos())).setPublishing(publishing).build();
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
