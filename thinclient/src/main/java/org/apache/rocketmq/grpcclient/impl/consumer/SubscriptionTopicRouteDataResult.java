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
import apache.rocketmq.v2.Status;
import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.Immutable;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.apis.exception.AuthenticationException;
import org.apache.rocketmq.apis.exception.AuthorisationException;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.exception.ResourceNotFoundException;
import org.apache.rocketmq.grpcclient.route.MessageQueueImpl;
import org.apache.rocketmq.grpcclient.route.TopicRouteDataResult;
import org.apache.rocketmq.grpcclient.utility.MixAll;

@Immutable
public class SubscriptionTopicRouteDataResult {
    private final AtomicInteger messageQueueIndex;

    private final Status status;

    private final ImmutableList<MessageQueueImpl> messageQueues;

    public SubscriptionTopicRouteDataResult(TopicRouteDataResult topicRouteDataResult) {
        this.messageQueueIndex = new AtomicInteger(RandomUtils.nextInt(0, Integer.MAX_VALUE));
        this.status = topicRouteDataResult.getStatus();
        final ImmutableList.Builder<MessageQueueImpl> builder = ImmutableList.builder();
        if (Code.OK != status.getCode()) {
            this.messageQueues = builder.build();
            return;
        }
        for (MessageQueueImpl messageQueue : topicRouteDataResult.getTopicRouteData().getMessageQueues()) {
            if (!messageQueue.getPermission().isReadable() ||
                MixAll.MASTER_BROKER_ID != messageQueue.getBroker().getId()) {
                continue;
            }
            builder.add(messageQueue);
        }
        this.messageQueues = builder.build();
    }

    public MessageQueueImpl takeMessageQueue() throws ClientException {
        final Code code = status.getCode();
        switch (code) {
            case OK:
                break;
            case FORBIDDEN:
                throw new AuthorisationException(code.getNumber(), status.getMessage());
            case UNAUTHORIZED:
                throw new AuthenticationException(code.getNumber(), status.getMessage());
            case TOPIC_NOT_FOUND:
                throw new ResourceNotFoundException(code.getNumber(), status.getMessage());
            case ILLEGAL_ACCESS_POINT:
                throw new IllegalArgumentException("Access point is illegal");
        }
        if (messageQueues.isEmpty()) {
            // TODO
            throw new AuthorisationException("Readable message queue is empty");
        }
        final int next = messageQueueIndex.getAndIncrement();
        return messageQueues.get(IntMath.mod(next, messageQueues.size()));
    }
}
