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

package org.apache.rocketmq.grpcclient.route;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.apis.exception.ResourceNotFoundException;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.grpcclient.utility.MixAll;

public class TopicRouteData {
    public static final TopicRouteData EMPTY = new TopicRouteData(Collections.emptyList());

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicRouteData.class);

    private final AtomicInteger index;
    /**
     * Partitions of topic route
     */
    private final ImmutableList<MessageQueueImpl> messageQueueImpls;

    /**
     * Construct topic route by partition list.
     *
     * @param messageQueues partition list, should never be empty.
     */
    public TopicRouteData(List<apache.rocketmq.v2.MessageQueue> messageQueues) {
        this.index = new AtomicInteger(RandomUtils.nextInt(0, Integer.MAX_VALUE));
        final ImmutableList.Builder<MessageQueueImpl> builder = ImmutableList.builder();
        for (apache.rocketmq.v2.MessageQueue partition : messageQueues) {
            builder.add(new MessageQueueImpl(partition));
        }
        this.messageQueueImpls = builder.build();
    }

    public Set<Endpoints> getTotalEndpoints() {
        Set<Endpoints> endpointsSet = new HashSet<>();
        for (MessageQueueImpl messageQueueImpl : messageQueueImpls) {
            endpointsSet.add(messageQueueImpl.getBroker().getEndpoints());
        }
        return endpointsSet;
    }

    public List<MessageQueueImpl> getMessageQueues() {
        return this.messageQueueImpls;
    }

    public Endpoints pickEndpointsToQueryAssignments() throws ResourceNotFoundException {
        int nextIndex = index.getAndIncrement();
        for (int i = 0; i < messageQueueImpls.size(); i++) {
            final MessageQueueImpl messageQueueImpl = messageQueueImpls.get(IntMath.mod(nextIndex++, messageQueueImpls.size()));
            final Broker broker = messageQueueImpl.getBroker();
            // TODO: polish magic code here.
            if (MixAll.MASTER_BROKER_ID != broker.getId()) {
                continue;
            }
            if (Permission.NONE == messageQueueImpl.getPermission()) {
                continue;
            }
            return broker.getEndpoints();
        }
        LOGGER.error("No available endpoints, topicRouteData={}", this);
        throw new ResourceNotFoundException("No available endpoints to pick for query assignments");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicRouteData that = (TopicRouteData) o;
        return Objects.equal(messageQueueImpls, that.messageQueueImpls);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(messageQueueImpls);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("partitions", messageQueueImpls)
                .toString();
    }
}
