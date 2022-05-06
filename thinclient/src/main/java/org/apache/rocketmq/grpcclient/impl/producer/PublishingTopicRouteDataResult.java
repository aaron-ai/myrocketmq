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

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Status;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.apis.exception.AuthenticationException;
import org.apache.rocketmq.apis.exception.AuthorisationException;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.exception.ResourceNotFoundException;
import org.apache.rocketmq.grpcclient.route.Broker;
import org.apache.rocketmq.grpcclient.route.Endpoints;
import org.apache.rocketmq.grpcclient.route.MessageQueueImpl;
import org.apache.rocketmq.grpcclient.route.TopicRouteDataResult;
import org.apache.rocketmq.grpcclient.utility.MixAll;

import javax.annotation.concurrent.Immutable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@Immutable
public class PublishingTopicRouteDataResult {
    private final AtomicInteger index;

    private final Status status;
    /**
     * Partitions to send message.
     */
    private final ImmutableList<MessageQueueImpl> messageQueues;

    public PublishingTopicRouteDataResult(TopicRouteDataResult topicRouteDataResult) {
        this.index = new AtomicInteger(RandomUtils.nextInt(0, Integer.MAX_VALUE));
        this.status = topicRouteDataResult.getStatus();
        final ImmutableList.Builder<MessageQueueImpl> builder = ImmutableList.builder();
        if (Code.OK != status.getCode()) {
            this.messageQueues = builder.build();
            return;
        }
        for (MessageQueueImpl messageQueue : topicRouteDataResult.getTopicRouteData().getMessageQueues()) {
            if (!messageQueue.getPermission().isWritable() ||
                MixAll.MASTER_BROKER_ID != messageQueue.getBroker().getId()) {
                continue;
            }
            builder.add(messageQueue);
        }
        this.messageQueues = builder.build();
    }

    public List<MessageQueueImpl> takeMessageQueues(Set<Endpoints> excluded, int count) throws ClientException {
        final Code code = status.getCode();
        switch (code) {
            case OK:
                break;
            case FORBIDDEN:
                throw new AuthorisationException(code.ordinal(), status.getMessage());
            case UNAUTHORIZED:
                throw new AuthenticationException(code.ordinal(), status.getMessage());
            case TOPIC_NOT_FOUND:
                throw new ResourceNotFoundException(code.ordinal(), status.getMessage());
            case ILLEGAL_ACCESS_POINT:
                throw new IllegalArgumentException("Access point is illegal");
        }
        int next = index.getAndIncrement();
        List<MessageQueueImpl> candidates = new ArrayList<>();
        Set<String> candidateBrokerNames = new HashSet<>();
        if (messageQueues.isEmpty()) {
            // TODO
            throw new AuthorisationException("Writable message queues is empty");
        }
        for (int i = 0; i < messageQueues.size(); i++) {
            final MessageQueueImpl messageQueueImpl = messageQueues.get(IntMath.mod(next++, messageQueues.size()));
            final Broker broker = messageQueueImpl.getBroker();
            final String brokerName = broker.getName();
            if (!excluded.contains(broker.getEndpoints()) && !candidateBrokerNames.contains(brokerName)) {
                candidateBrokerNames.add(brokerName);
                candidates.add(messageQueueImpl);
            }
            if (candidates.size() >= count) {
                return candidates;
            }
        }
        // If all endpoints are isolated.
        if (candidates.isEmpty()) {
            for (int i = 0; i < messageQueues.size(); i++) {
                final MessageQueueImpl messageQueueImpl = messageQueues.get(IntMath.mod(next++, messageQueues.size()));
                final Broker broker = messageQueueImpl.getBroker();
                final String brokerName = broker.getName();
                if (!candidateBrokerNames.contains(brokerName)) {
                    candidateBrokerNames.add(brokerName);
                    candidates.add(messageQueueImpl);
                }
                if (candidates.size() >= count) {
                    break;
                }
            }
        }
        return candidates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PublishingTopicRouteDataResult that = (PublishingTopicRouteDataResult) o;
        return Objects.equal(messageQueues, that.messageQueues);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(messageQueues);
    }
}
