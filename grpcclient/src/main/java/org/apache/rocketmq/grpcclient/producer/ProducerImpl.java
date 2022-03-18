package org.apache.rocketmq.grpcclient.producer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.message.Message;
import org.apache.rocketmq.apis.producer.Producer;
import org.apache.rocketmq.apis.producer.SendReceipt;
import org.apache.rocketmq.apis.producer.Transaction;
import org.apache.rocketmq.apis.producer.TransactionChecker;
import org.apache.rocketmq.apis.retry.BackoffRetryPolicy;

import static com.google.common.base.Preconditions.checkNotNull;

public class ProducerImpl implements Producer {
    private ClientConfiguration clientConfiguration;
    private Set<String> topics = new HashSet<>();
    private int asyncThreadCount;
    private BackoffRetryPolicy retryPolicy;
    private TransactionChecker checker;

    public ProducerImpl(ClientConfiguration clientConfiguration, Set<String> topics, int asyncThreadCount,
                        BackoffRetryPolicy retryPolicy, TransactionChecker checker) {
        this.clientConfiguration = checkNotNull(clientConfiguration, "clientConfiguration should not be null");
        this.topics = checkNotNull(topics, "topics should not be null");
        this.asyncThreadCount = asyncThreadCount;
        this.retryPolicy = checkNotNull(retryPolicy, "retryPolicy should not be null");
        this.checker = checker;
    }

    void start() throws ClientException {
    }

    @Override
    public SendReceipt send(Message message) throws ClientException {
        return null;
    }

    @Override
    public SendReceipt send(Message message, Transaction transaction) throws ClientException {
        return null;
    }

    @Override
    public CompletableFuture<SendReceipt> sendAsync(Message message) {
        return null;
    }

    @Override
    public List<SendReceipt> send(List<Message> messages) throws ClientException {
        return null;
    }

    @Override
    public Transaction beginTransaction() throws ClientException {
        return null;
    }

    @Override
    public void close() {

    }
}
