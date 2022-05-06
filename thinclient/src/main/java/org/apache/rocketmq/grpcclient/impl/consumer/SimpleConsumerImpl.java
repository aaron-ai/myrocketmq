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
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.VerifyMessageCommand;
import com.google.common.base.Function;
import com.google.common.math.IntMath;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import io.grpc.Metadata;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import net.javacrumbs.futureconverter.java8guava.FutureConverter;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.consumer.FilterExpression;
import org.apache.rocketmq.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.apis.exception.AuthenticationException;
import org.apache.rocketmq.apis.exception.AuthorisationException;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.exception.InternalException;
import org.apache.rocketmq.apis.exception.ResourceNotFoundException;
import org.apache.rocketmq.apis.message.MessageView;
import org.apache.rocketmq.grpcclient.consumer.ReceiveMessageResult;
import org.apache.rocketmq.grpcclient.message.protocol.Resource;
import org.apache.rocketmq.grpcclient.route.Endpoints;
import org.apache.rocketmq.grpcclient.route.MessageQueueImpl;
import org.apache.rocketmq.grpcclient.route.TopicRouteDataResult;

@SuppressWarnings("UnstableApiUsage")
public class SimpleConsumerImpl extends ConsumerImpl implements SimpleConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumerImpl.class);

    private final ClientConfiguration clientConfiguration;
    private final SimpleConsumerSettings simpleConsumerSettings;
    private final String consumerGroup;
    private final Duration awaitDuration;

    private final AtomicInteger topicIndex;

    private final Map<String /* topic */, FilterExpression> subscriptionExpressions;
    private final ConcurrentMap<String /* topic */, SubscriptionTopicRouteDataResult> subscriptionTopicRouteDataResultCache;

    public SimpleConsumerImpl(ClientConfiguration clientConfiguration, String consumerGroup, Duration awaitDuration,
        Map<String, FilterExpression> subscriptionExpressions) {
        super(clientConfiguration, consumerGroup, subscriptionExpressions.keySet());
        this.clientConfiguration = clientConfiguration;
        Resource groupResource = new Resource(consumerGroup);
        this.simpleConsumerSettings = new SimpleConsumerSettings(clientId, accessEndpoints, groupResource, clientConfiguration.getRequestTimeout(), awaitDuration, subscriptionExpressions);
        this.consumerGroup = consumerGroup;
        this.awaitDuration = awaitDuration;

        this.topicIndex = new AtomicInteger(RandomUtils.nextInt(0, Integer.MAX_VALUE));

        this.subscriptionExpressions = subscriptionExpressions;
        this.subscriptionTopicRouteDataResultCache = new ConcurrentHashMap<>();
    }

    @Override
    public String getConsumerGroup() {
        return consumerGroup;
    }

    @Override
    public SimpleConsumer subscribe(String topic, FilterExpression filterExpression) throws ClientException {
        final ListenableFuture<TopicRouteDataResult> future = getRouteDataResult(topic);
        TopicRouteDataResult topicRouteDataResult;
        try {
            topicRouteDataResult = future.get();
        } catch (InterruptedException e) {
            throw new InternalException(e);
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof ClientException) {
                throw (ClientException) cause;
            }
            // TODO:
            throw new InternalException(e);
        }
        final Status status = topicRouteDataResult.getStatus();
        final Code code = status.getCode();
        switch (code) {
            case OK:
                subscriptionExpressions.put(topic, filterExpression);
                return this;
            case TOPIC_NOT_FOUND:
                throw new ResourceNotFoundException(code.ordinal(), status.getMessage());
            case UNAUTHORIZED:
                throw new AuthenticationException(code.ordinal(), status.getMessage());
            case FORBIDDEN:
                throw new AuthorisationException(code.ordinal(), status.getMessage());
            default:
                throw new InternalException(code.ordinal(), status.getMessage());
        }
    }

    @Override
    public SimpleConsumer unsubscribe(String topic) throws ClientException {
        subscriptionExpressions.remove(topic);
        return this;
    }

    @Override
    public Map<String, FilterExpression> getSubscriptionExpressions() {
        return new HashMap<>(subscriptionExpressions);
    }

    @Override
    public List<MessageView> receive(int maxMessageNum, Duration invisibleDuration) throws ClientException {
        final ListenableFuture<List<MessageView>> future = receive0(maxMessageNum, invisibleDuration);
        try {
            return future.get();
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            final Throwable cause = t.getCause();
            if (cause instanceof ClientException) {
                throw (ClientException) cause;
            }
            throw new InternalException(t);
        }
    }

    @Override
    public CompletableFuture<List<MessageView>> receiveAsync(int maxMessageNum, Duration invisibleDuration) {
        final ListenableFuture<List<MessageView>> future = receive0(maxMessageNum, invisibleDuration);
        return FutureConverter.toCompletableFuture(future);
    }

    public ListenableFuture<List<MessageView>> receive0(int maxMessageNum, Duration invisibleDuration) {
        final HashMap<String, FilterExpression> copy = new HashMap<>(subscriptionExpressions);
        final ArrayList<String> topics = new ArrayList<>(copy.keySet());
        if (topics.isEmpty()) {
            // TODO:
            throw new IllegalArgumentException("");
        }
        final String topic = topics.get(IntMath.mod(topicIndex.getAndIncrement(), topics.size()));
        final FilterExpression filterExpression = copy.get(topic);
        final ListenableFuture<SubscriptionTopicRouteDataResult> routeFuture = getSubscriptionTopicRouteResult(topic);
        final ListenableFuture<ReceiveMessageResult> future0 = Futures.transformAsync(routeFuture, result -> {
            final MessageQueueImpl mq = result.takeMessageQueue();
            final ReceiveMessageRequest request = wrapReceiveMessageRequest(maxMessageNum, mq, filterExpression, invisibleDuration);
            return receiveMessage(request, mq, awaitDuration);
        }, MoreExecutors.directExecutor());
        return Futures.transformAsync(future0, result -> {
            SettableFuture<List<MessageView>> future = SettableFuture.create();
            final Optional<Status> optionalStatus = result.getStatus();
            if (!optionalStatus.isPresent()) {
                future.set(new ArrayList<>(result.getMessages()));
                return future;
            }
            final Status status = optionalStatus.get();
            final Code code = status.getCode();
            // TODO
            switch (code) {
                case OK:
                    future.set(new ArrayList<>(result.getMessages()));
                    return future;
                case UNAUTHORIZED:
                    throw new AuthenticationException(code.getNumber(), status.getMessage());
                case FORBIDDEN:
                    throw new AuthorisationException(code.getNumber(), status.getMessage());
                default:
                    throw new InternalException(code.getNumber(), status.getMessage());
            }
        }, MoreExecutors.directExecutor());
    }

    @Override
    public void ack(MessageView messageView) throws ClientException {

    }

    @Override
    public CompletableFuture<Void> ackAsync(MessageView messageView) {
        return null;
    }

    private ListenableFuture<Void> ack0(MessageView messageView) {

    }

    @Override
    public void changeInvisibleDuration(MessageView messageView, Duration invisibleDuration) throws ClientException {

    }

    @Override
    public CompletableFuture<Void> changeInvisibleDurationAsync(MessageView messageView, Duration invisibleDuration) {
        return null;
    }

    @Override
    public void close() throws IOException {
        this.stopAsync().awaitTerminated();
    }

    @Override
    public void applySettings(Endpoints endpoints, Settings settings) {

    }

    @Override
    public void onVerifyMessageCommand(Endpoints endpoints, VerifyMessageCommand verifyMessageCommand) {

    }

    @Override
    public void onRecoverOrphanedTransactionCommand(Endpoints endpoints,
        RecoverOrphanedTransactionCommand recoverOrphanedTransactionCommand) {

    }

    @Override
    protected void awaitFirstSettingApplied(
        Duration duration) throws ExecutionException, InterruptedException, TimeoutException {
        simpleConsumerSettings.getFirstApplyCompletedFuture().get(duration.toNanos(), TimeUnit.NANOSECONDS);

    }

    @Override
    public Settings localSettings() {
        return simpleConsumerSettings.toProtobuf();
    }

    public void onTopicRouteDataResultUpdate0(String topic, TopicRouteDataResult topicRouteDataResult) {
        final SubscriptionTopicRouteDataResult subscriptionTopicRouteDataResult = new SubscriptionTopicRouteDataResult(topicRouteDataResult);
        subscriptionTopicRouteDataResultCache.put(topic, subscriptionTopicRouteDataResult);
    }

    private ListenableFuture<SubscriptionTopicRouteDataResult> getSubscriptionTopicRouteResult(final String topic) {
        SettableFuture<SubscriptionTopicRouteDataResult> future0 = SettableFuture.create();
        final SubscriptionTopicRouteDataResult result = subscriptionTopicRouteDataResultCache.get(topic);
        if (null != result) {
            future0.set(result);
            return future0;
        }
        final ListenableFuture<TopicRouteDataResult> future = getRouteDataResult(topic);
        return Futures.transform(future, topicRouteDataResult -> {
            final SubscriptionTopicRouteDataResult subscriptionTopicRouteDataResult = new SubscriptionTopicRouteDataResult(topicRouteDataResult);
            subscriptionTopicRouteDataResultCache.put(topic, subscriptionTopicRouteDataResult);
            return subscriptionTopicRouteDataResult;
        }, MoreExecutors.directExecutor());
    }
}
