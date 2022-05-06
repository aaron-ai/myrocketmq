package org.apache.rocketmq.grpcclient.example;

import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.ClientServiceProvider;
import org.apache.rocketmq.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.apis.consumer.FilterExpression;
import org.apache.rocketmq.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.message.MessageView;

public class SimpleConsumerExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumerExample.class);

    public static void main(String[] args) throws ClientException {
        String accessPoint = "ipv4:11.166.42.94:8081";
        String topic = "lingchu_normal_topic";
        String tag = "tagA";
        String consumerGroup = "lingchu_normal_group";
        String accessKey = "AccessKey";
        String secretKey = "secretKey";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);

        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        StaticSessionCredentialsProvider staticSessionCredentialsProvider = new StaticSessionCredentialsProvider(accessKey, secretKey);
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setAccessPoint(accessPoint)
            .setCredentialProvider(staticSessionCredentialsProvider)
            .build();

        final SimpleConsumer consumer = provider.newSimpleConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            .setConsumerGroup(consumerGroup)
            .setAwaitDuration(Duration.ofSeconds(30))
            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
            .build();

        while (true) {
            final List<MessageView> messageViews = consumer.receive(1, Duration.ofSeconds(5));
            LOGGER.info("Received message size={}, messageId(s)={}", messageViews.size(), messageViews.stream().map(MessageView::getMessageId).collect(Collectors.toList()));
            for (MessageView messageView : messageViews) {
                consumer.ack(messageView);
                LOGGER.info("Ack message successfully, messageId={}", messageView.getMessageId());
            }
        }
    }
}
