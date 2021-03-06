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

import apache.rocketmq.v2.CustomizedBackoff;
import apache.rocketmq.v2.ExponentialBackoff;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.RetryPolicy;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SubscriptionEntry;
import com.google.common.base.MoreObjects;
import com.google.protobuf.util.Durations;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.rocketmq.apis.consumer.FilterExpression;
import org.apache.rocketmq.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.apis.retry.ExponentialBackoffRetryPolicy;
import org.apache.rocketmq.grpcclient.impl.ClientSettings;
import org.apache.rocketmq.grpcclient.impl.ClientType;
import org.apache.rocketmq.grpcclient.impl.CustomizedBackoffRetryPolicy;
import org.apache.rocketmq.grpcclient.message.protocol.Resource;
import org.apache.rocketmq.grpcclient.route.Endpoints;

public class PushConsumerSettings extends ClientSettings {
    private static final Logger LOGGER = LoggerFactory.getLogger(PushConsumerSettings.class);

    private final Resource group;
    private final Map<String, FilterExpression> subscriptionExpressions;
    private volatile Boolean fifo = false;
    private volatile int receiveBatchSize = 32;
    private volatile Duration longPollingTimeout = Duration.ofSeconds(30);

    public PushConsumerSettings(String clientId, Endpoints accessPoint, Resource group, Duration requestTimeout,
        Map<String, FilterExpression> subscriptionExpression) {
        super(clientId, ClientType.PUSH_CONSUMER, accessPoint, requestTimeout);
        this.group = group;
        this.subscriptionExpressions = subscriptionExpression;
    }

    public boolean isFifo() {
        return fifo;
    }

    public int getReceiveBatchSize() {
        return receiveBatchSize;
    }

    public Duration getLongPollingTimeout() {
        return longPollingTimeout;
    }

    @Override
    public Settings toProtobuf() {
        List<SubscriptionEntry> subscriptionEntries = new ArrayList<>();
        for (Map.Entry<String, FilterExpression> entry : subscriptionExpressions.entrySet()) {
            final FilterExpression filterExpression = entry.getValue();
            apache.rocketmq.v2.Resource topic = apache.rocketmq.v2.Resource.newBuilder().setName(entry.getKey()).build();
            final apache.rocketmq.v2.FilterExpression.Builder expressionBuilder = apache.rocketmq.v2.FilterExpression.newBuilder().setExpression(filterExpression.getExpression());
            final FilterExpressionType type = filterExpression.getFilterExpressionType();
            switch (type) {
                case TAG:
                    expressionBuilder.setType(FilterType.TAG);
                    break;
                case SQL92:
                    expressionBuilder.setType(FilterType.SQL);
                    break;
                default:
                    LOGGER.warn("[Bug] Unrecognized filter type, type={}", type);
            }
            SubscriptionEntry subscriptionEntry = SubscriptionEntry.newBuilder().setTopic(topic).setExpression(expressionBuilder.build()).build();
            subscriptionEntries.add(subscriptionEntry);
        }
        Subscription subscription = Subscription.newBuilder().setGroup(group.toProtobuf()).addAllSubscriptions(subscriptionEntries).build();
        return Settings.newBuilder().setAccessPoint(accessPoint.toProtobuf()).setClientType(clientType.toProtobuf())
            .setRequestTimeout(Durations.fromNanos(requestTimeout.toNanos())).setSubscription(subscription).build();
    }

    @Override
    public void applySettings(Settings settings) {
        final Settings.PubSubCase pubSubCase = settings.getPubSubCase();
        if (!Settings.PubSubCase.SUBSCRIPTION.equals(pubSubCase)) {
            LOGGER.error("[Bug] Issued settings not match with the client type, client id ={}, pub-sub case={}, client type={}", clientId, pubSubCase, clientType);
            return;
        }
        final Subscription subscription = settings.getSubscription();
        this.fifo = subscription.getFifo();
        this.receiveBatchSize = subscription.getReceiveBatchSize();
        this.longPollingTimeout = Duration.ofNanos(Durations.toNanos(subscription.getLongPollingTimeout()));
        final RetryPolicy backoffPolicy = settings.getBackoffPolicy();
        switch (backoffPolicy.getStrategyCase()) {
            case EXPONENTIAL_BACKOFF:
                final ExponentialBackoff exponentialBackoff = backoffPolicy.getExponentialBackoff();
                retryPolicy = new ExponentialBackoffRetryPolicy(backoffPolicy.getMaxAttempts(), Duration.ofNanos(exponentialBackoff.getInitial().getNanos()), Duration.ofNanos(exponentialBackoff.getMax().getNanos()), exponentialBackoff.getMultiplier());
                break;
            case CUSTOMIZED_BACKOFF:
                final CustomizedBackoff customizedBackoff = backoffPolicy.getCustomizedBackoff();
                retryPolicy = new CustomizedBackoffRetryPolicy(customizedBackoff.getNextList().stream().map(duration -> Duration.ofNanos(Durations.toNanos(duration))).collect(Collectors.toList()), backoffPolicy.getMaxAttempts());
                break;
            default:
                throw new IllegalArgumentException("Unrecognized backoff policy strategy.");
        }
        this.firstApplyCompletedFuture.set(this);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("clientId", clientId)
            .add("clientType", clientType)
            .add("accessPoint", accessPoint)
            .add("retryPolicy", retryPolicy)
            .add("requestTimeout", requestTimeout)
            .add("group", group)
            .add("subscriptionExpressions", subscriptionExpressions)
            .add("fifo", fifo)
            .add("receiveBatchSize", receiveBatchSize)
            .add("longPollingTimeout", longPollingTimeout)
            .toString();
    }
}
