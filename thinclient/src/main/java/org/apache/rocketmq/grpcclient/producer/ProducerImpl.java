/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.grpcclient.producer;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;
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

@SuppressWarnings("UnstableApiUsage")
public class ProducerImpl extends ClientImpl implements Producer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerImpl.class);

    private final Set<String> topics;
    private final int sendAsyncThreadCount;
    private final BackoffRetryPolicy retryPolicy;
    private final TransactionChecker checker;

    private final ExecutorService sendAsyncExecutor;

    /**
     * The caller is supposed to have validated the arguments and handled throwing exception or
     * logging warnings already, so we avoid repeating args check here.
     */
    ProducerImpl(ClientConfiguration clientConfiguration, Set<String> topics, int sendAsyncThreadCount,
                 BackoffRetryPolicy retryPolicy, TransactionChecker checker) {
        super(clientConfiguration);
        this.topics = topics;
        this.sendAsyncThreadCount = sendAsyncThreadCount;
        this.retryPolicy = retryPolicy;
        this.checker = checker;

        this.sendAsyncExecutor = new ThreadPoolExecutor(
                sendAsyncThreadCount,
                sendAsyncThreadCount,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryImpl("SendAsyncWorker"));
    }

    @Override
    protected void startUp() {
        LOGGER.info("Begin to start the rocketmq producer, clientId={}", clientId);
        super.startUp();
        LOGGER.info("The rocketmq producer starts successfully, clientId={}", clientId);
    }

    @Override
    protected void shutDown() throws InterruptedException {
        LOGGER.info("Begin to shutdown the rocketmq producer, clientId={}", clientId);
        super.shutDown();
        if (!ExecutorServices.awaitTerminated(sendAsyncExecutor)) {
            LOGGER.error("[Bug] Failed to shutdown default send async executor, clientId={}", clientId);
        }
        LOGGER.info("Shutdown the rocketmq producer successfully, clientId={}", clientId);
    }

    /**
     * @see Producer#send(Message)
     */
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

    /**
     * @see Producer#send(Message, Transaction)
     */
    @Override
    public SendReceipt send(Message message, Transaction transaction) throws ClientException {
        return null;
    }

    /**
     * @see Producer#sendAsync(Message)
     */
    @Override
    public CompletableFuture<SendReceipt> sendAsync(Message message) {
        return null;
    }

    /**
     * @see Producer#send(List)
     */
    @Override
    public List<SendReceipt> send(List<Message> messages) throws ClientException {
        return null;
    }

    /**
     * @see Producer#beginTransaction()
     */
    @Override
    public Transaction beginTransaction() throws ClientException {
        return null;
    }

    @Override
    public void close() {
        this.stopAsync().awaitTerminated();
    }

    private ListenableFuture<SendReceipt> send0(final Message message) {
        topics.add(message.getTopic());
        return null;
    }
}
