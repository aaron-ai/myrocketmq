package org.apache.rocketmq.grpcclient.example;

import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.io.IOException;
import java.security.acl.LastOwnerException;
import java.util.Collections;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.ClientServiceProvider;
import org.apache.rocketmq.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.apis.consumer.ConsumeResult;
import org.apache.rocketmq.apis.consumer.FilterExpression;
import org.apache.rocketmq.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.apis.consumer.PushConsumer;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.message.MessageView;

public class PushConsumerExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerExample.class);

    public static void main(String[] args) throws InterruptedException {
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
        try (PushConsumer ignored = provider.newPushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            .setConsumerGroup(consumerGroup)
            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
            .setMessageListener(messageView -> {
                LOGGER.info("Received message, message={}", messageView);
                return ConsumeResult.OK;
            })
            .build()) {
            LOGGER.info("Start push consumer successfully.");
            Thread.sleep(1000000);
        } catch (IOException | ClientException e) {
            e.printStackTrace();
        }
    }
}
