package org.apache.rocketmq.grpcclient.example;

import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ReadOnlyBufferException;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.ClientServiceProvider;
import org.apache.rocketmq.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.message.Message;
import org.apache.rocketmq.apis.producer.Producer;
import org.apache.rocketmq.apis.producer.SendReceipt;

public class ProducerExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerExample.class);

    public static void main(String[] args) {
        try {
            String accessPoint = "ipv4:11.166.42.94:8081";
            String topic = "lingchu_normal_topic";
            String tag = "tagA";
            byte[] body = "Hello RocketMQ".getBytes(StandardCharsets.UTF_8);
            String accessKey = "AccessKey";
            String secretKey = "SecretKey";

            final ClientServiceProvider provider = ClientServiceProvider.loadService();
            StaticSessionCredentialsProvider staticSessionCredentialsProvider = new StaticSessionCredentialsProvider(accessKey, secretKey);
            ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setAccessPoint(accessPoint)
                .setCredentialProvider(staticSessionCredentialsProvider)
                .build();
            final Message message = provider.newMessageBuilder()
                .setTopic(topic)
                .setBody(body)
                .setTag(tag)
                .build();
            try (Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setTopics(topic)
                .build()) {
                LOGGER.info("Start producer successfully.");
                final SendReceipt sendReceipt = producer.send(message);
                LOGGER.info("Send message successfully, sendReceipt={}", sendReceipt);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        } catch (ReadOnlyBufferException t) {
            System.out.println(t);
        }
    }
}
