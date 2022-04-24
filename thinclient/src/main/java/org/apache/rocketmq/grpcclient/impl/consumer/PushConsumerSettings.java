package org.apache.rocketmq.grpcclient.impl.consumer;

import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import com.google.protobuf.util.Durations;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Map;
import org.apache.rocketmq.apis.consumer.FilterExpression;
import org.apache.rocketmq.grpcclient.impl.ClientSettings;
import org.apache.rocketmq.grpcclient.impl.ClientType;
import org.apache.rocketmq.grpcclient.message.protocol.Resource;
import org.apache.rocketmq.grpcclient.route.Endpoints;

public class PushConsumerSettings extends ClientSettings {
    private static final Logger LOGGER = LoggerFactory.getLogger(PushConsumerSettings.class);

    private final Resource group;
    private final Map<String, FilterExpression> subscriptionExpressions;
    private volatile Boolean fifo = false;
    private volatile int receiveBatchSize = 32;
    private volatile Duration longPollingTimeout = Duration.ofSeconds(30);

    public PushConsumerSettings(String clientId, Endpoints accessPoint, Resource group, Duration requestTimeout,
        Map<String, FilterExpression> subscriptionExpression) {
        super(clientId, ClientType.PUSH_CONSUMER, accessPoint, requestTimeout);
        this.group = group;
        this.subscriptionExpressions = subscriptionExpression;
    }

    public boolean isFifo() {
        return fifo;
    }

    public int getReceiveBatchSize() {
        return receiveBatchSize;
    }

    public Duration getLongPollingTimeout() {
        return longPollingTimeout;
    }

    // TODO: set subscription table.
    @Override
    public Settings toProtobuf() {
        Subscription subscription = Subscription.newBuilder().setGroup(group.toProtobuf()).build();
        return Settings.newBuilder().setAccessPoint(accessPoint.toProtobuf()).setClientType(clientType.toProtobuf())
            .setRequestTimeout(Durations.fromNanos(requestTimeout.toNanos())).setSubscription(subscription).build();
    }

    @Override
    public void applySettings(Settings settings) {
        final Settings.PubSubCase pubSubCase = settings.getPubSubCase();
        if (!Settings.PubSubCase.SUBSCRIPTION.equals(pubSubCase)) {
            LOGGER.error("[Bug] Issued settings not match with the client type, client id ={}, pub-sub case={}, client type={}", clientId, pubSubCase, clientType);
            return;
        }
        final Subscription subscription = settings.getSubscription();
        this.fifo = subscription.getFifo();
        this.receiveBatchSize = subscription.getReceiveBatchSize();
        this.longPollingTimeout = Duration.ofNanos(Durations.toNanos(subscription.getLongPollingTimeout()));
    }
}
