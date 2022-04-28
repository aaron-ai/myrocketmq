package org.apache.rocketmq.grpcclient.impl.consumer;

import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.VerifyMessageCommand;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.consumer.FilterExpression;
import org.apache.rocketmq.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.message.MessageView;
import org.apache.rocketmq.grpcclient.message.protocol.Resource;
import org.apache.rocketmq.grpcclient.route.Endpoints;

public class SimpleConsumerImpl extends ConsumerImpl implements SimpleConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumerImpl.class);

    private final ClientConfiguration clientConfiguration;
    private final SimpleConsumerSettings simpleConsumerSettings;
    private final String consumerGroup;
    private final Duration awaitDuration;
    private final Map<String /* topic */, FilterExpression> subscriptionExpressions;

    public SimpleConsumerImpl(ClientConfiguration clientConfiguration, String consumerGroup, Duration awaitDuration,
        Map<String, FilterExpression> subscriptionExpressions) {
        super(clientConfiguration, consumerGroup, subscriptionExpressions.keySet());
        this.clientConfiguration = clientConfiguration;
        Resource groupResource = new Resource(namespace, consumerGroup);
        this.simpleConsumerSettings = new SimpleConsumerSettings(clientId, accessEndpoints, groupResource, clientConfiguration.getRequestTimeout(), awaitDuration, subscriptionExpressions);
        this.consumerGroup = consumerGroup;
        this.awaitDuration = awaitDuration;
        this.subscriptionExpressions = subscriptionExpressions;
    }

    @Override
    public String getConsumerGroup() {
        return consumerGroup;
    }

    @Override
    public SimpleConsumer subscribe(String topic, FilterExpression filterExpression) throws ClientException {
        return null;
    }

    @Override
    public SimpleConsumer unsubscribe(String topic) throws ClientException {
        return null;
    }

    @Override
    public Map<String, FilterExpression> getSubscriptionExpressions() {
        return new HashMap<>(subscriptionExpressions);
    }

    @Override
    public List<MessageView> receive(int maxMessageNum, Duration invisibleDuration) throws ClientException {
        return null;
    }

    @Override
    public CompletableFuture<List<MessageView>> receiveAsync(int maxMessageNum,
        Duration invisibleDuration) throws ClientException {
        return null;
    }

    @Override
    public void ack(MessageView messageView) throws ClientException {

    }

    @Override
    public CompletableFuture<Void> ackAsync(MessageView messageView) {
        return null;
    }

    @Override
    public void changeInvisibleDuration(MessageView messageView, Duration invisibleDuration) throws ClientException {

    }

    @Override
    public CompletableFuture<Void> changeInvisibleDurationAsync(MessageView messageView, Duration invisibleDuration) {
        return null;
    }

    @Override
    public void close() throws IOException {
        this.stopAsync().awaitTerminated();
    }

    @Override
    public void applySettings(Endpoints endpoints, Settings settings) {

    }

    @Override
    public void onVerifyMessageCommand(Endpoints endpoints, VerifyMessageCommand verifyMessageCommand) {

    }

    @Override
    public void onRecoverOrphanedTransactionCommand(Endpoints endpoints,
        RecoverOrphanedTransactionCommand recoverOrphanedTransactionCommand) {

    }

    @Override
    protected void awaitFirstSettingApplied(
        Duration duration) throws ExecutionException, InterruptedException, TimeoutException {
        simpleConsumerSettings.getFirstApplyCompletedFuture().get(duration.toNanos(), TimeUnit.NANOSECONDS);

    }

    @Override
    public Settings localSettings() {
        return null;
    }

    @Override
    public NotifyClientTerminationRequest wrapNotifyClientTerminationRequest() {
        return null;
    }
}
