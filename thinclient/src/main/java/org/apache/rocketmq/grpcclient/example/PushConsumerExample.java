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

    public static void main(String[] args) {
        String accessPoint = "http://MQ_INST_1080056302921134_BXxiFN4R.mq.cn-shenzhen.aliyuncs.com:80";
        String topic = "lingchu-test-topic";
        String tag = "tagA";
        String consumerGroup = "lingchu-test-group";
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
            .setMessageListener(messageView -> ConsumeResult.OK)
            .build()) {
            LOGGER.info("Start push consumer successfully.");
        } catch (IOException | ClientException e) {
            e.printStackTrace();
        }
    }
}
