package org.apache.rocketmq.grpcclient.producer;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.message.Message;
import org.apache.rocketmq.apis.producer.Producer;
import org.apache.rocketmq.apis.producer.SendReceipt;
import org.apache.rocketmq.apis.producer.Transaction;
import org.apache.rocketmq.apis.producer.TransactionChecker;
import org.apache.rocketmq.apis.retry.BackoffRetryPolicy;
import org.apache.rocketmq.grpcclient.impl.ClientImpl;
import org.apache.rocketmq.grpcclient.utility.ExecutorServices;
import org.apache.rocketmq.grpcclient.utility.ThreadFactoryImpl;

import static com.google.common.base.Preconditions.checkNotNull;

@SuppressWarnings("UnstableApiUsage")
public class ProducerImpl extends ClientImpl implements Producer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerImpl.class);

    private final Set<String> topics;
    private final int asyncThreadCount;
    private final BackoffRetryPolicy retryPolicy;
    private final TransactionChecker checker;

    private final ExecutorService sendAsyncExecutor;

    public ProducerImpl(ClientConfiguration clientConfiguration, Set<String> topics, int asyncThreadCount,
                        BackoffRetryPolicy retryPolicy, TransactionChecker checker) {
        super(clientConfiguration);
        this.topics = checkNotNull(topics, "topics should not be null");
        this.asyncThreadCount = asyncThreadCount;
        this.retryPolicy = checkNotNull(retryPolicy, "retryPolicy should not be null");
        this.checker = checker;

        this.sendAsyncExecutor = new ThreadPoolExecutor(
                asyncThreadCount,
                asyncThreadCount,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryImpl("SendAsyncWorker"));
    }

    @Override
    protected void startUp() {
        LOGGER.info("Begin to start the rocketmq producer, clientId={}", id);
        super.startUp();
        LOGGER.info("The rocketmq producer starts successfully, clientId={}", id);
    }

    @Override
    protected void shutDown() throws InterruptedException {
        LOGGER.info("Begin to shutdown the rocketmq producer, clientId={}", id);
        super.shutDown();
        if (!ExecutorServices.awaitTerminated(sendAsyncExecutor)) {
            LOGGER.error("[Bug] Failed to shutdown default send async executor, clientId={}", id);
        }
        LOGGER.info("Shutdown the rocketmq producer successfully, clientId={}", id);
    }

    @Override
    public SendReceipt send(Message message) throws ClientException {
        final CompletableFuture<SendReceipt> future = sendAsync(message);
        try {
            return future.get();
        } catch (Throwable ignore) {
            // TODO: polish code
            return null;
        }
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
        this.stopAsync().awaitTerminated();
    }
}
