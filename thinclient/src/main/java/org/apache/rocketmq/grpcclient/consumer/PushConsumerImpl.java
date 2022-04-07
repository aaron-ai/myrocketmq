package org.apache.rocketmq.grpcclient.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.MessageQueue;
import org.apache.rocketmq.apis.consumer.FilterExpression;
import org.apache.rocketmq.apis.consumer.MessageListener;
import org.apache.rocketmq.apis.consumer.PushConsumer;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.grpcclient.impl.consumer.ConsumeService;
import org.apache.rocketmq.grpcclient.impl.consumer.FifoConsumeService;
import org.apache.rocketmq.grpcclient.impl.consumer.ProcessQueue;

public class PushConsumerImpl implements PushConsumer {
    private final ClientConfiguration clientConfiguration;
    private final String consumerGroup;
    private final boolean enableFifoConsumption;
    private final Map<String, FilterExpression> subscriptionExpressions;
    private final MessageListener messageListener;
    private final int maxBatchSize;
    private final int maxCacheMessageCount;
    private final int maxCacheMessageSizeInBytes;
    private final int consumptionThreadCount;

//    private final ConsumeService consumeService;
    private final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable;

    /**
     * The caller is supposed to have validated the arguments and handled throwing exception or
     * logging warnings already, so we avoid repeating args check here.
     */
    public PushConsumerImpl(ClientConfiguration clientConfiguration, String consumerGroup,
        boolean enableFifoConsumption, Map<String, FilterExpression> subscriptionExpressions,
        MessageListener messageListener, int maxBatchSize, int maxCacheMessageCount, int maxCacheMessageSizeInBytes,
        int consumptionThreadCount) {
        this.clientConfiguration = clientConfiguration;
        this.consumerGroup = consumerGroup;
        this.enableFifoConsumption = enableFifoConsumption;
        this.subscriptionExpressions = subscriptionExpressions;
        this.messageListener = messageListener;
        this.maxBatchSize = maxBatchSize;
        this.maxCacheMessageCount = maxCacheMessageCount;
        this.maxCacheMessageSizeInBytes = maxCacheMessageSizeInBytes;
        this.consumptionThreadCount = consumptionThreadCount;

        this.processQueueTable = new ConcurrentHashMap<>();
    }

    /**
     * @see PushConsumer#getConsumerGroup()
     */
    @Override
    public String getConsumerGroup() {
        return consumerGroup;
    }

    /**
     * @see PushConsumer#getSubscriptionExpressions()
     */
    @Override
    public Map<String, FilterExpression> getSubscriptionExpressions() {
        return new HashMap<>(subscriptionExpressions);
    }

    /**
     * @see PushConsumer#subscribe(String, FilterExpression)
     */
    @Override
    public PushConsumer subscribe(String topic, FilterExpression filterExpression) throws ClientException {
        return null;
    }

    /**
     * @see PushConsumer#unsubscribe(String)
     */
    @Override
    public PushConsumer unsubscribe(String topic) throws ClientException {
        return null;
    }

    /**
     * @see PushConsumer#close()
     */
    @Override
    public void close() {
    }
}
