package org.apache.rocketmq.grpcclient.example;

import org.apache.rocketmq.apis.ClientServiceProvider;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.message.Message;
import org.apache.rocketmq.apis.producer.Producer;
import org.apache.rocketmq.apis.producer.SendReceipt;

public class ProducerExample {
    public static void main(String[] args) throws ClientException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        final Message message = provider.newMessageBuilder().build();
        final Producer producer = provider.newProducerBuilder().build();
        final SendReceipt sendReceipt = producer.send(message);
    }
}
