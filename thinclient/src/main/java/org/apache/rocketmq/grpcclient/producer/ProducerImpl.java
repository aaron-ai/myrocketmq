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

import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import com.google.common.base.Function;
import com.google.common.math.IntMath;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.grpc.Metadata;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.message.Message;
import org.apache.rocketmq.apis.message.MessageId;
import org.apache.rocketmq.apis.producer.Producer;
import org.apache.rocketmq.apis.producer.SendReceipt;
import org.apache.rocketmq.apis.producer.Transaction;
import org.apache.rocketmq.apis.producer.TransactionChecker;
import org.apache.rocketmq.apis.retry.BackoffRetryPolicy;
import org.apache.rocketmq.grpcclient.impl.ClientImpl;
import org.apache.rocketmq.grpcclient.message.PublishingMessageImpl;
import org.apache.rocketmq.grpcclient.route.Endpoints;
import org.apache.rocketmq.grpcclient.route.MessageQueueImpl;
import org.apache.rocketmq.grpcclient.route.TopicRouteDataResult;
import org.apache.rocketmq.grpcclient.utility.ExecutorServices;
import org.apache.rocketmq.grpcclient.utility.ThreadFactoryImpl;

import static com.google.common.base.Preconditions.checkNotNull;

@SuppressWarnings({"UnstableApiUsage", "NullableProblems"})
public class ProducerImpl extends ClientImpl implements Producer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerImpl.class);
    /**
     * If message body size exceeds the threshold, it would be compressed for convenience of transport.
     */
    public static final int MESSAGE_COMPRESSION_BYTES_THRESHOLD = 1024 * 4;

    /**
     * The default GZIP compression level for message body.
     */
    public static final int MESSAGE_COMPRESSION_LEVEL = 5;

    private final Set<String> topics;
    private final int sendAsyncThreadCount;
    private final BackoffRetryPolicy retryPolicy;
    private final TransactionChecker checker;
    private final ConcurrentMap<String/* topic */, PublishingTopicRouteDataResult> publishingRouteDataResultCache;

    @GuardedBy("isolatedLock")
    private final Set<Endpoints> isolated;
    private final ReadWriteLock isolatedLock;

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

        this.isolated = new HashSet<>();
        this.isolatedLock = new ReentrantReadWriteLock();

        this.publishingRouteDataResultCache = new ConcurrentHashMap<>();
        this.sendAsyncExecutor = new ThreadPoolExecutor(
            sendAsyncThreadCount,
            sendAsyncThreadCount,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("SendAsyncWorker"));
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Begin to close the rocketmq producer, clientId={}", clientId);
        super.close();
        try {
            if (!ExecutorServices.awaitTerminated(sendAsyncExecutor)) {
                LOGGER.error("[Bug] Failed to shutdown default send async executor, clientId={}", clientId);
            }
        } catch (InterruptedException e) {
            LOGGER.error("Exception raised while closing the rocketmq producer, clientId={}", clientId);
            throw new IOException("Exception raised while closing the rocketmq producer", e);
        }
        LOGGER.info("Shutdown the rocketmq producer successfully, clientId={}", clientId);
    }

    @Override
    public HeartbeatRequest wrapHeartbeatRequest() {
        return HeartbeatRequest.newBuilder().build();
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
        final ListenableFuture<SendReceipt> future = send0(message, false);
        // TODO
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

    private List<MessageQueueImpl> takeMessageQueues(PublishingTopicRouteDataResult result) throws ClientException {
        Set<Endpoints> isolated = new HashSet<>();
        isolatedLock.readLock().lock();
        try {
            isolated.addAll(this.isolated);
        } finally {
            isolatedLock.readLock().unlock();
        }
        return result.takeMessageQueues(isolated, retryPolicy.getMaxAttempts());
    }

    private ListenableFuture<SendReceipt> send0(final Message message, boolean transactionEnabled) {
        final ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        final ListenableFuture<List<SendReceipt>> future = send0(messages, transactionEnabled);
        return Futures.transform(future, (Function<List<SendReceipt>, SendReceipt>)
            // List size is 1 for single message.
            sendReceipts -> sendReceipts.iterator().next());
    }

    private ListenableFuture<List<SendReceipt>> send0(final List<Message> messages, boolean transactionEnabled) {
        SettableFuture<List<SendReceipt>> future = SettableFuture.create();
        // Collect topics to send message.
        final Set<String> topics = messages.stream().map(Message::getTopic).collect(Collectors.toSet());
        if (1 != topics.size()) {
            // Messages have different topics, no need to proceed.
            final IllegalArgumentException e = new IllegalArgumentException("Messages to send have different topic");
            future.setException(e);
            LOGGER.error("Messages to send have different topics, no need to proceed, topics={}", topics, e);
            return future;
        }
        // TODO: notify server.
        final String topic = topics.iterator().next();
        this.topics.add(topic);

        List<PublishingMessageImpl> publishingMessages = new ArrayList<>();
        for (Message message : messages) {
            try {
                publishingMessages.add(new PublishingMessageImpl(namespace, message, transactionEnabled));
            } catch (IOException e) {
                // Failed to refine message, no need to proceed.
                LOGGER.error("Failed to refine message, namespace={}, topic={}, clientId={}, message={}", namespace,
                    topic, clientId, message, e);
                future.setException(e);
                return future;
            }
        }
        // Get publishing topic route.
        final ListenableFuture<PublishingTopicRouteDataResult> routeFuture = getPublishingTopicRouteResult(topic);
        return Futures.transformAsync(routeFuture, result -> {
            // Prepare the candidate partitions for retry-sending in advance.
            final List<MessageQueueImpl> candidates = takeMessageQueues(result);
            return send0(publishingMessages, candidates);
        });
    }

    private ListenableFuture<List<SendReceipt>> send0(final List<PublishingMessageImpl> publishingMessages,
        List<MessageQueueImpl> candidates) {
        final SettableFuture<List<SendReceipt>> future = SettableFuture.create();
        send0(future, candidates, publishingMessages, 1);
        return future;
    }

    /**
     * The caller is supposed to make sure different messages have the same message type and same topic.
     */
    private SendMessageRequest wrapSendMessageRequest(List<PublishingMessageImpl> messageImpls,
        MessageQueueImpl messageQueue) {
        // The caller is supposed to have validated that all messages have the same topic.
        final String topic = messageImpls.iterator().next().getTopic();
        final Resource topicResource = Resource.newBuilder().setResourceNamespace(namespace)
            .setName(topic).build();

        Broker broker = Broker.newBuilder().setName(messageQueue.getBroker().getName()).build();
        MessageQueue mq = MessageQueue.newBuilder().setTopic(topicResource).setBroker(broker).build();

        final SendMessageRequest.Builder requestBuilder = SendMessageRequest.newBuilder().setMessageQueue(mq);
        for (PublishingMessageImpl messageImpl : messageImpls) {
            requestBuilder.addMessages(messageImpl.toProtobuf());
        }
        return requestBuilder.build();
    }

    private void send0(SettableFuture<List<SendReceipt>> future, final List<MessageQueueImpl> candidates,
        final List<PublishingMessageImpl> messages, final int attempt) {
        Metadata metadata;
        try {
            metadata = sign();
        } catch (Throwable t) {
            // failed to sign, no need to proceed.
            future.setException(t);
            return;
        }
        // calculate the current partition.
        final MessageQueueImpl messageQueue = candidates.get(IntMath.mod(attempt - 1, candidates.size()));
        final Endpoints endpoints = messageQueue.getBroker().getEndpoints();
        final SendMessageRequest request = wrapSendMessageRequest(messages, messageQueue);

        final ListenableFuture<SendMessageResponse> responseFuture =
            clientManager.sendMessage(endpoints, metadata, request, clientConfiguration.getRequestTimeout());

        final ListenableFuture<List<SendReceipt>> attemptFuture = Futures.transformAsync(responseFuture, response -> {
                final SettableFuture<List<SendReceipt>> future0 = SettableFuture.create();
                // TODO: may throw exception.
                future0.set(SendReceiptImpl.processSendResponse(messageQueue, checkNotNull(response)));
                return future0;
            }
        );

        final int maxAttempts = retryPolicy.getMaxAttempts();
        Futures.addCallback(attemptFuture, new FutureCallback<List<SendReceipt>>() {
            @Override
            public void onSuccess(List<SendReceipt> sendReceipts) {
                if (sendReceipts.size() != messages.size()) {
                    LOGGER.error("[Bug] Due to an unknown reason from server, received send receipts' quantity[{}]" +
                        " is not equal to messages' quantity[{}]", sendReceipts.size(), messages.size());
                }
                // No need more attempts.
                future.set(sendReceipts);

                // resend message(s) successfully.
                if (1 < attempt) {
                    // Collect successful messageId(s) for logging.
                    List<MessageId> messageIds = new ArrayList<>();
                    for (SendReceipt receipt : sendReceipts) {
                        messageIds.add(receipt.getMessageId());
                    }
                    LOGGER.info("Resend message successfully, namespace={}, topic={}, messageId(s)={}, maxAttempts={}, "
                            + "attempt={}, endpoints={}, clientId={}", namespace, messageQueue.getTopic(), messageIds, maxAttempts, attempt,
                        endpoints, clientId);
                }
                // send message on first attempt, return directly.
            }

            @Override
            public void onFailure(Throwable t) {
                if (attempt >= maxAttempts) {
                    // Mo need more attempts.
                    future.setException(t);
                    // Collect successful messageId(s) for logging.
                    List<MessageId> messageIds = new ArrayList<>();
                    for (PublishingMessageImpl message : messages) {
                        messageIds.add(message.getMessageId());
                    }
                    LOGGER.error("Failed to send message(s) finally, run out of attempt times, maxAttempts={}, " +
                            "attempt={}, namespace={}, topic={}, messageId(s)={}, endpoints={}, clientId={}",
                        maxAttempts, attempt, namespace, messageQueue.getTopic(), messageIds, endpoints, clientId);
                    return;
                }
                // Try to do more attempts.
                int nextAttempt = 1 + attempt;
                final Duration delay = retryPolicy.getNextAttemptDelay(nextAttempt);
                clientManager.getScheduler().schedule(() -> send0(future, candidates, messages, nextAttempt),
                    delay.getNano(), TimeUnit.NANOSECONDS);
            }
        });
    }

    private ListenableFuture<PublishingTopicRouteDataResult> getPublishingTopicRouteResult(final String topic) {
        SettableFuture<PublishingTopicRouteDataResult> future0 = SettableFuture.create();
        final PublishingTopicRouteDataResult publishingSendingRouteData = publishingRouteDataResultCache.get(topic);
        if (null != publishingSendingRouteData) {
            future0.set(publishingSendingRouteData);
            return future0;
        }
        final ListenableFuture<TopicRouteDataResult> future = getRouteDataResult(topic);
        return Futures.transform(future, (Function<TopicRouteDataResult, PublishingTopicRouteDataResult>) topicRouteDataResult -> {
            final PublishingTopicRouteDataResult publishingTopicRouteDataResult = new PublishingTopicRouteDataResult(topicRouteDataResult);
            publishingRouteDataResultCache.put(topic, publishingTopicRouteDataResult);
            return publishingTopicRouteDataResult;
        });
    }
}
