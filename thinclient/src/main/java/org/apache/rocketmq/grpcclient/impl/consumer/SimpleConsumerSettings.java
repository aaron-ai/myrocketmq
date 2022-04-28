package org.apache.rocketmq.grpcclient.impl.consumer;

import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SubscriptionEntry;
import com.google.protobuf.util.Durations;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.apis.consumer.FilterExpression;
import org.apache.rocketmq.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.grpcclient.impl.ClientSettings;
import org.apache.rocketmq.grpcclient.impl.ClientType;
import org.apache.rocketmq.grpcclient.message.protocol.Resource;
import org.apache.rocketmq.grpcclient.route.Endpoints;

public class SimpleConsumerSettings extends ClientSettings {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumerSettings.class);

    private final Resource group;
    private final Duration longPollingTimeout;
    private final Map<String, FilterExpression> subscriptionExpressions;

    public SimpleConsumerSettings(String clientId, Endpoints accessPoint, Resource group, Duration requestTimeout,
        Duration longPollingTimeout, Map<String, FilterExpression> subscriptionExpression) {
        super(clientId, ClientType.SIMPLE_CONSUMER, accessPoint, requestTimeout);
        this.group = group;
        this.subscriptionExpressions = subscriptionExpression;
        this.longPollingTimeout = longPollingTimeout;
    }

    @Override
    public Settings toProtobuf() {
        List<SubscriptionEntry> subscriptionEntries = new ArrayList<>();
        for (Map.Entry<String, FilterExpression> entry : subscriptionExpressions.entrySet()) {
            final FilterExpression filterExpression = entry.getValue();
            apache.rocketmq.v2.Resource topic = apache.rocketmq.v2.Resource.newBuilder().setName(entry.getKey()).build();
            final apache.rocketmq.v2.FilterExpression.Builder expressionBuilder = apache.rocketmq.v2.FilterExpression.newBuilder().setExpression(filterExpression.getExpression());
            final FilterExpressionType type = filterExpression.getFilterExpressionType();
            switch (type) {
                case TAG:
                    expressionBuilder.setType(FilterType.TAG);
                    break;
                case SQL92:
                    expressionBuilder.setType(FilterType.SQL);
                    break;
                default:
                    LOGGER.warn("[Bug] Unrecognized filter type for simple consumer, type={}", type);
            }
            SubscriptionEntry subscriptionEntry = SubscriptionEntry.newBuilder().setTopic(topic).setExpression(expressionBuilder.build()).build();
            subscriptionEntries.add(subscriptionEntry);
        }
        Subscription subscription = Subscription.newBuilder().setGroup(group.toProtobuf())
            .setLongPollingTimeout(Durations.fromNanos(longPollingTimeout.toNanos()))
            .addAllSubscriptions(subscriptionEntries).build();
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
        this.firstApplyCompletedFuture.set(null);
    }
}
