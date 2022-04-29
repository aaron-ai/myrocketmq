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

package org.apache.rocketmq.grpcclient.impl.consumer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.Status;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.apis.consumer.ConsumeResult;
import org.apache.rocketmq.apis.consumer.FilterExpression;
import org.apache.rocketmq.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.apis.message.MessageView;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.grpcclient.consumer.ReceiveMessageResult;
import org.apache.rocketmq.grpcclient.route.Endpoints;
import org.apache.rocketmq.grpcclient.route.MessageQueueImpl;

/**
 * @see ProcessQueue
 */
@SuppressWarnings("NullableProblems")
public class ProcessQueueImpl implements ProcessQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessQueueImpl.class);

    public static final Duration RECEIVE_LONG_POLLING_DURATION = Duration.ofSeconds(30);
    public static final Duration RECEIVE_LATER_DELAY = Duration.ofSeconds(3);
    public static final Duration MAX_IDLE_DURATION = Duration.ofNanos(2 * RECEIVE_LONG_POLLING_DURATION.toNanos());
    public static final int RECEPTION_BATCH_SIZE = 32;

    private final PushConsumerImpl consumer;

    /**
     * Dropped means {@link ProcessQueue} is deprecated, which means no message would be fetched from remote anymore.
     */
    private volatile boolean dropped;
    private final MessageQueueImpl mq;
    private final FilterExpression filterExpression;

    /**
     * Messages which is pending means have been cached, but are not taken by consumer dispatcher yet.
     */
    @GuardedBy("pendingMessagesLock")
    private final List<MessageView> pendingMessages;
    private final ReadWriteLock pendingMessagesLock;

    /**
     * Message which is in-flight means have been dispatched, but the consumption process is not accomplished.
     */
    @GuardedBy("inflightMessagesLock")
    private final List<MessageView> inflightMessages;
    private final ReadWriteLock inflightMessagesLock;

    private final AtomicLong cachedMessagesBytes;

    private volatile long activityNanoTime = System.nanoTime();

    public ProcessQueueImpl(PushConsumerImpl consumer, MessageQueueImpl mq, FilterExpression filterExpression) {
        this.consumer = consumer;
        this.dropped = false;
        this.mq = mq;
        this.filterExpression = filterExpression;
        this.pendingMessages = new ArrayList<>();
        this.pendingMessagesLock = new ReentrantReadWriteLock();
        this.inflightMessages = new ArrayList<>();
        this.inflightMessagesLock = new ReentrantReadWriteLock();
        this.cachedMessagesBytes = new AtomicLong();
    }

    @Override
    public MessageQueueImpl getMessageQueue() {
        return mq;
    }

    @Override
    public void drop() {
        this.dropped = true;
    }

    @Override
    public boolean expired() {
        final Duration idleDuration = Duration.ofNanos(System.nanoTime() - activityNanoTime);
        if (idleDuration.compareTo(MAX_IDLE_DURATION) > 0) {
            return false;
        }

        LOGGER.warn("Process queue is idle, idle duration={}, max idle time={}ms, mq={}, "
            + "clientId={}", idleDuration, MAX_IDLE_DURATION, mq, consumer.getClientId());
        return true;
    }

    public void cacheMessages(List<MessageView> messageList) {
        pendingMessagesLock.writeLock().lock();
        try {
            for (MessageView message : messageList) {
                pendingMessages.add(message);
                cachedMessagesBytes.addAndGet(message.getBody().remaining());
            }
        } finally {
            pendingMessagesLock.writeLock().unlock();
        }
    }

    private int getReceptionBatchSize() {
        int bufferSize = consumer.cacheMessageCountThresholdPerQueue() - this.cachedMessagesCount();
        bufferSize = Math.max(bufferSize, 1);
        return Math.min(bufferSize, RECEPTION_BATCH_SIZE);
    }

    private ReceiveMessageRequest wrapReceiveMessageRequest() {
        final int receptionBatchSize = getReceptionBatchSize();
        return ReceiveMessageRequest.newBuilder()
            .setGroup(consumer.getProtobufGroup())
            .setMessageQueue(mq.toProtobuf())
            .setFilterExpression(getProtoBufFilterExpression())
            .setBatchSize(receptionBatchSize).build();
    }

    private apache.rocketmq.v2.FilterExpression getProtoBufFilterExpression() {
        final FilterExpressionType expressionType = filterExpression.getFilterExpressionType();

        apache.rocketmq.v2.FilterExpression.Builder expressionBuilder =
            apache.rocketmq.v2.FilterExpression.newBuilder();

        final String expression = filterExpression.getExpression();
        expressionBuilder.setExpression(expression);
        switch (expressionType) {
            case SQL92:
                return expressionBuilder.setType(FilterType.SQL).build();
            case TAG:
            default:
                return expressionBuilder.setType(FilterType.TAG).build();
        }
    }

    @Override
    public void fetchMessageImmediately() {
        receiveMessageImmediately();
    }

    /**
     * Receive message later by message queue.
     *
     * <p> Make sure that no exception will be thrown.
     */
    public void receiveMessageLater() {
        final ScheduledExecutorService scheduler = consumer.getScheduler();
        try {
            scheduler.schedule(this::receiveMessage, RECEIVE_LATER_DELAY.toNanos(), TimeUnit.NANOSECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            // Should never reach here.
            LOGGER.error("[Bug] Failed to schedule receive message request, namespace={}, mq={}, clientId={}", mq, consumer.getClientId(), t);
            receiveMessageLater();
        }
    }

    public void receiveMessage() {
        if (dropped) {
            LOGGER.info("Process queue has been dropped, no longer receive message, namespace={}, mq={}, clientId={}", mq, consumer.getClientId());
            return;
        }
        if (this.isCacheFull()) {
            LOGGER.warn("Process queue cache is full, would receive message later, namespace={}, mq={}, clientId={}", mq, consumer.getClientId());
            receiveMessageLater();
            return;
        }
        receiveMessageImmediately();
    }

    private void receiveMessageImmediately() {
        // TODO: check client status.
        try {
            final Endpoints endpoints = mq.getBroker().getEndpoints();
            final ReceiveMessageRequest request = wrapReceiveMessageRequest();
            activityNanoTime = System.nanoTime();
            final ListenableFuture<ReceiveMessageResult> future = consumer.receiveMessage(request, mq, RECEIVE_LONG_POLLING_DURATION);
            Futures.addCallback(future, new FutureCallback<ReceiveMessageResult>() {
                @Override
                public void onSuccess(ReceiveMessageResult result) {
                    try {
                        onReceiveMessageResult(result);
                    } catch (Throwable t) {
                        // Should never reach here.
                        LOGGER.error("[Bug] Exception raised while handling receive result, would receive later, "
                            + "namespace={}, mq={}, endpoints={}, clientId={}", mq, endpoints, consumer.getClientId(), t);
                        receiveMessageLater();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    LOGGER.error("Exception raised while message reception, would receive later, namespace={}, mq={}, "
                        + "endpoints={}, clientId={}", mq, endpoints, consumer.getClientId(), t);
                    receiveMessageLater();
                }
            }, MoreExecutors.directExecutor());
            consumer.getReceptionTimes().getAndIncrement();
        } catch (Throwable t) {
            LOGGER.error("Exception raised while message reception, would receive later, mq={}, clientId={}", mq, consumer.getClientId(), t);
            receiveMessageLater();
        }
    }

    public boolean isCacheFull() {
        final int cacheMessageCountThresholdPerQueue = consumer.cacheMessageCountThresholdPerQueue();
        final long actualMessagesQuantity = this.cachedMessagesCount();
        if (cacheMessageCountThresholdPerQueue <= actualMessagesQuantity) {
            LOGGER.warn("Process queue total cached messages quantity exceeds the threshold, threshold={}, actual={}, mq={}, clientId={}", cacheMessageCountThresholdPerQueue,
                actualMessagesQuantity, mq, consumer.getClientId());
            return true;
        }
        final int cacheMessageBytesThresholdPerQueue = consumer.cacheMessageBytesThresholdPerQueue();
        final long actualCachedMessagesBytes = this.cachedMessageBytes();
        if (cacheMessageBytesThresholdPerQueue <= actualCachedMessagesBytes) {
            LOGGER.warn("Process queue total cached messages memory exceeds the threshold, threshold={} bytes, actual={} "
                    + "bytes, mq={}, clientId={}", cacheMessageBytesThresholdPerQueue,
                actualCachedMessagesBytes, mq, consumer.getClientId());
            return true;
        }
        return false;
    }

    public int cachedMessagesCount() {
        pendingMessagesLock.readLock().lock();
        inflightMessagesLock.readLock().lock();
        try {
            return pendingMessages.size() + inflightMessages.size();
        } finally {
            inflightMessagesLock.readLock().unlock();
            pendingMessagesLock.readLock().unlock();
        }
    }

    public long cachedMessageBytes() {
        return cachedMessagesBytes.get();
    }

    private void onReceiveMessageResult(ReceiveMessageResult result) {
        Optional<Status> status = result.getStatus();
        final List<MessageView> messages = result.getMessages();
        final Endpoints endpoints = result.getEndpoints();
        if (!status.isPresent()) {
            // Should not reach here.
            LOGGER.error("[Bug] Status in receive message result is not set, mq={}, endpoints={}", mq, endpoints);
            if (messages.isEmpty()) {
                receiveMessage();
            }
            status = Optional.of(Status.newBuilder().setCode(Code.OK).build());
            LOGGER.error("[Bug] Status not set but message(s) found in the receive result, fix the status to OK, mq={}, endpoints={}", mq, endpoints);
        }
        final Code code = status.get().getCode();
        switch (code) {
            case OK:
                if (!messages.isEmpty()) {
                    cacheMessages(messages);
                    consumer.getReceivedMessagesQuantity().getAndAdd(messages.size());
                    consumer.getConsumeService().signal();
                }
                LOGGER.debug("Receive message with OK, mq={}, endpoints={}, messages found count={}, clientId={}", mq, endpoints, messages.size(), consumer.getClientId());
                receiveMessage();
                break;
            // Fall through on purpose.
            case UNAUTHORIZED:
            case FORBIDDEN:
            case TOO_MANY_REQUESTS:
            default:
                receiveMessageLater();
        }
    }

    @Override
    public Optional<MessageView> tryTakeMessage() {
        pendingMessagesLock.writeLock().lock();
        inflightMessagesLock.writeLock().lock();
        try {
            final Optional<MessageView> first = pendingMessages.stream().findFirst();
            if (!first.isPresent()) {
                return first;
            }
            final MessageView messageView = first.get();
            inflightMessages.add(messageView);
            pendingMessages.remove(messageView);
            return first;
        } finally {
            inflightMessagesLock.writeLock().unlock();
            pendingMessagesLock.writeLock().lock();
        }
    }

    private void eraseMessages(List<MessageView> messageViews) {
        inflightMessagesLock.writeLock().lock();
        try {
            for (MessageView messageView : messageViews) {
                if (inflightMessages.remove(messageView)) {
                    cachedMessagesBytes.addAndGet(-messageView.getBody().remaining());
                }
            }
        } finally {
            inflightMessagesLock.writeLock().unlock();
        }
    }

    @Override
    public void eraseMessage(MessageView messageView, ConsumeResult consumeResult) {
    }

    @Override
    public Optional<MessageView> tryTakeFifoMessage() {
        return Optional.empty();
    }

    @Override
    public void eraseFifoMessage(MessageView message, ConsumeResult consumeResult) {
    }

    @Override
    public void doStats() {

    }
}
