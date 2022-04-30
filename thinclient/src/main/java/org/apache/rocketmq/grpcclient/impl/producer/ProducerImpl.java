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

package org.apache.rocketmq.grpcclient.impl.producer;

import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.VerifyMessageCommand;
import com.google.common.math.IntMath;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.grpc.Metadata;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import net.javacrumbs.futureconverter.java8guava.FutureConverter;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.message.Message;
import org.apache.rocketmq.apis.message.MessageId;
import org.apache.rocketmq.apis.producer.Producer;
import org.apache.rocketmq.apis.producer.SendReceipt;
import org.apache.rocketmq.apis.producer.Transaction;
import org.apache.rocketmq.apis.producer.TransactionChecker;
import org.apache.rocketmq.apis.retry.ExponentialBackoffRetryPolicy;
import org.apache.rocketmq.grpcclient.impl.ClientImpl;
import org.apache.rocketmq.grpcclient.message.MessageType;
import org.apache.rocketmq.grpcclient.message.PublishingMessageImpl;
import org.apache.rocketmq.grpcclient.message.protocol.Resource;
import org.apache.rocketmq.grpcclient.route.Endpoints;
import org.apache.rocketmq.grpcclient.route.MessageQueueImpl;
import org.apache.rocketmq.grpcclient.route.TopicRouteDataResult;
import org.apache.rocketmq.grpcclient.utility.ExecutorServices;
import org.apache.rocketmq.grpcclient.utility.ThreadFactoryImpl;

@SuppressWarnings({"UnstableApiUsage", "NullableProblems"})
public class ProducerImpl extends ClientImpl implements Producer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerImpl.class);

    private final ProducerSettings producerSettings;

    private final int sendAsyncThreadCount;
    private final ExponentialBackoffRetryPolicy retryPolicy;
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
        ExponentialBackoffRetryPolicy retryPolicy, TransactionChecker checker) {
        super(clientConfiguration, topics);
        this.producerSettings = new ProducerSettings(clientId, accessEndpoints, retryPolicy, clientConfiguration.getRequestTimeout(), topics.stream().map(Resource::new).collect(Collectors.toSet()));
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

    public int getSendAsyncThreadCount() {
        return sendAsyncThreadCount;
    }

    @Override
    public void applySettings(Endpoints endpoints, Settings settings) {
        producerSettings.applySettings(settings);
    }

    @Override
    public void onVerifyMessageCommand(Endpoints endpoints, VerifyMessageCommand verifyMessageCommand) {
        LOGGER.warn("Ignore verify message command from remote, which is not expected for producer, client id={}, command={}", clientId, verifyMessageCommand);
    }

    @Override
    public void onRecoverOrphanedTransactionCommand(Endpoints endpoints,
        RecoverOrphanedTransactionCommand recoverOrphanedTransactionCommand) {
        // TODO
    }

    @Override
    public Settings localSettings() {
        return producerSettings.toProtobuf();
    }

    @Override
    public NotifyClientTerminationRequest wrapNotifyClientTerminationRequest() {
        return NotifyClientTerminationRequest.newBuilder().build();
    }

    @Override
    protected void awaitFirstSettingApplied(
        Duration duration) throws ExecutionException, InterruptedException, TimeoutException {
        producerSettings.getFirstApplyCompletedFuture().get(duration.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void shutDown() throws InterruptedException {
        LOGGER.info("Begin to shutdown the rocketmq producer, clientId={}", clientId);
        super.shutDown();
        sendAsyncExecutor.shutdown();
        if (!ExecutorServices.awaitTerminated(sendAsyncExecutor)) {
            LOGGER.error("[Bug] Failed to shutdown default send async executor, clientId={}", clientId);
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
        // TODO: polish code.
        try {
            return future.get();
        } catch (Exception t) {
            final Throwable cause = t.getCause();
            if (cause instanceof ClientException) {
                throw (ClientException) cause;
            }

        }
        return null;
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
        final ListenableFuture<SendReceipt> future = Futures.transform(send0(Collections.singletonList(message), false),
            sendReceipts -> sendReceipts.iterator().next(), MoreExecutors.directExecutor());
        return FutureConverter.toCompletableFuture(future);
    }

    /**
     * @see Producer#send(List)
     */
    @Override
    public List<SendReceipt> send(List<Message> messages) throws ClientException {
        final ListenableFuture<List<SendReceipt>> future = send0(messages, false);
        try {
            final List<SendReceipt> sendReceipts = future.get();
        } catch (Throwable t) {
            // TODO: polish code
            System.out.println(t);
        }
        // TODO
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
    public void close() throws IOException {
        this.stopAsync().awaitTerminated();
    }

    /**
     * Isolate specified {@link Endpoints}.
     */
    private void isolate(Endpoints endpoints) {
        isolatedLock.writeLock().lock();
        try {
            isolated.add(endpoints);
        } finally {
            isolatedLock.writeLock().unlock();
        }
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

    private ListenableFuture<List<SendReceipt>> send0(List<Message> messages, boolean txEnabled) {
        SettableFuture<List<SendReceipt>> future = SettableFuture.create();
        List<PublishingMessageImpl> pubMessages = new ArrayList<>();
        for (Message message : messages) {
            try {
                final PublishingMessageImpl pubMessage = new PublishingMessageImpl(message, txEnabled);
                pubMessages.add(pubMessage);
            } catch (Throwable t) {
                // Failed to refine message, no need to proceed.
                LOGGER.error("Failed to refine message, clientId={}, message={}", clientId, message, t);
                future.setException(t);
                return future;
            }
        }
        // Collect topics to send message.
        final Set<String> topics = pubMessages.stream().map(Message::getTopic).collect(Collectors.toSet());
        if (1 < topics.size()) {
            // Messages have different topics, no need to proceed.
            final IllegalArgumentException e = new IllegalArgumentException("Messages to send have different topics");
            future.setException(e);
            LOGGER.error("Messages to send have different topics, no need to proceed, topics={}", topics, e);
            return future;
        }

        // Collect message types.
        final Set<MessageType> messageTypes = pubMessages.stream()
            .map(PublishingMessageImpl::getMessageType)
            .collect(Collectors.toSet());
        if (1 < messageTypes.size()) {
            // Messages have different message type, no need to proceed.
            final IllegalArgumentException e = new IllegalArgumentException("Messages to send have different types");
            future.setException(e);
            LOGGER.error("Messages to send have different types, no need to proceed, types={}", messageTypes, e);
            return future;
        }

        final String topic = topics.iterator().next();
        final MessageType messageType = messageTypes.iterator().next();

        this.topics.add(topic);
        // Get publishing topic route.
        final ListenableFuture<PublishingTopicRouteDataResult> routeFuture = getPublishingTopicRouteResult(topic);
        return Futures.transformAsync(routeFuture, result -> {
            // Prepare the candidate partitions for retry-sending in advance.
            final List<MessageQueueImpl> candidates = takeMessageQueues(result);
            final SettableFuture<List<SendReceipt>> future0 = SettableFuture.create();
            send0(future0, topic, messageType, candidates, pubMessages, 1);
            return future0;
        }, sendAsyncExecutor);
    }

    /**
     * The caller is supposed to make sure different messages have the same message type and same topic.
     */
    private SendMessageRequest wrapSendMessageRequest(List<PublishingMessageImpl> messages) {
        return SendMessageRequest.newBuilder()
            .addAllMessages(messages.stream().map(PublishingMessageImpl::toProtobuf).collect(Collectors.toList()))
            .build();
    }

    private void send0(SettableFuture<List<SendReceipt>> future, String topic, MessageType messageType,
        final List<MessageQueueImpl> candidates, final List<PublishingMessageImpl> messages, final int attempt) {
        Metadata metadata;
        try {
            metadata = sign();
        } catch (Throwable t) {
            // Failed to sign, no need to proceed.
            future.setException(t);
            return;
        }
        // Calculate the current partition.
        final MessageQueueImpl messageQueue = candidates.get(IntMath.mod(attempt - 1, candidates.size()));
        final Endpoints endpoints = messageQueue.getBroker().getEndpoints();
        final SendMessageRequest request = wrapSendMessageRequest(messages);

        final ListenableFuture<SendMessageResponse> responseFuture = clientManager.sendMessage(endpoints, metadata,
            request, clientConfiguration.getRequestTimeout());

        final ListenableFuture<List<SendReceipt>> attemptFuture = Futures.transformAsync(responseFuture, response -> {
            final SettableFuture<List<SendReceipt>> future0 = SettableFuture.create();
            // TODO: may throw exception.
            future0.set(SendReceiptImpl.processSendResponse(messageQueue, response));
            return future0;
        }, MoreExecutors.directExecutor());

        final int maxAttempts = retryPolicy.getMaxAttempts();
        Futures.addCallback(attemptFuture, new FutureCallback<List<SendReceipt>>() {
            @Override
            public void onSuccess(List<SendReceipt> sendReceipts) {
                if (sendReceipts.size() != messages.size()) {
                    LOGGER.error("[Bug] Due to an unknown reason from remote, received send receipts' quantity[{}]" +
                        " is not equal to messages' quantity[{}]", sendReceipts.size(), messages.size());
                }
                // No need more attempts.
                future.set(sendReceipts);
                // Resend message(s) successfully.
                if (1 < attempt) {
                    // Collect messageId(s) for logging.
                    List<MessageId> messageIds = new ArrayList<>();
                    for (SendReceipt receipt : sendReceipts) {
                        messageIds.add(receipt.getMessageId());
                    }
                    LOGGER.info("Resend message successfully, topic={}, messageId(s)={}, maxAttempts={}, "
                            + "attempt={}, endpoints={}, clientId={}", topic, messageIds, maxAttempts, attempt,
                        endpoints, clientId);
                }
                // Send message(s) successfully on first attempt, return directly.
            }

            @Override
            public void onFailure(Throwable t) {
                // Collect messageId(s) for logging.
                List<MessageId> messageIds = new ArrayList<>();
                for (PublishingMessageImpl message : messages) {
                    messageIds.add(message.getMessageId());
                }
                // Isolate endpoints because of sending failure.
                isolate(endpoints);
                if (attempt >= maxAttempts) {
                    // No need more attempts.
                    future.setException(t);
                    LOGGER.error("Failed to send message(s) finally, run out of attempt times, maxAttempts={}, " +
                            "attempt={}, topic={}, messageId(s)={}, endpoints={}, clientId={}",
                        maxAttempts, attempt, topic, messageIds, endpoints, clientId, t);
                    return;
                }
                // No need more attempts for transactional message.
                if (MessageType.TRANSACTION.equals(messageType)) {
                    future.setException(t);
                    LOGGER.error("Failed to send transactional message finally, maxAttempts=1, attempt={}, " +
                        "topic={}, messageId(s), endpoints={}, clientId={}", attempt, topic, messageIds, endpoints, clientId, t);
                    return;
                }
                // Try to do more attempts.
                int nextAttempt = 1 + attempt;
                final Duration delay = retryPolicy.getNextAttemptDelay(nextAttempt);
                LOGGER.warn("Failed to send message, would attempt to resend after {}, maxAttempts={}," +
                        " attempt={}, topic={}, messageId(s)={}, endpoints={}, clientId={}", delay, maxAttempts, attempt,
                    topic, messageIds, endpoints, clientId, t);
                clientManager.getScheduler().schedule(() -> send0(future, topic, messageType, candidates, messages, nextAttempt),
                    delay.toNanos(), TimeUnit.NANOSECONDS);
            }
        }, MoreExecutors.directExecutor());
    }

    private ListenableFuture<PublishingTopicRouteDataResult> getPublishingTopicRouteResult(final String topic) {
        SettableFuture<PublishingTopicRouteDataResult> future0 = SettableFuture.create();
        final PublishingTopicRouteDataResult publishingSendingRouteData = publishingRouteDataResultCache.get(topic);
        if (null != publishingSendingRouteData) {
            future0.set(publishingSendingRouteData);
            return future0;
        }
        final ListenableFuture<TopicRouteDataResult> future = getRouteDataResult(topic);
        return Futures.transform(future, topicRouteDataResult -> {
            final PublishingTopicRouteDataResult publishingTopicRouteDataResult = new PublishingTopicRouteDataResult(topicRouteDataResult);
            publishingRouteDataResultCache.put(topic, publishingTopicRouteDataResult);
            return publishingTopicRouteDataResult;
        }, MoreExecutors.directExecutor());
    }
}
