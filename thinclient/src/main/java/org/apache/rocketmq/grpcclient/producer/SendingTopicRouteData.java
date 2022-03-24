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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.grpcclient.message.MessageQueueImpl;
import org.apache.rocketmq.grpcclient.route.Broker;
import org.apache.rocketmq.grpcclient.route.Endpoints;
import org.apache.rocketmq.grpcclient.route.Partition;
import org.apache.rocketmq.grpcclient.route.TopicRouteData;
import org.apache.rocketmq.grpcclient.utility.MixAll;

import javax.annotation.concurrent.Immutable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@Immutable
public class SendingTopicRouteData {
    private static final Logger LOGGER = LoggerFactory.getLogger(SendingTopicRouteData.class);

    private final AtomicInteger index;
    /**
     * Partitions to send message.
     */
    private final ImmutableList<Partition> partitions;

    public SendingTopicRouteData(TopicRouteData topicRouteData) {
        this.index = new AtomicInteger(RandomUtils.nextInt(0, Integer.MAX_VALUE));
        this.partitions = filterPartition(topicRouteData);
    }

    public List<MessageQueueImpl> getMessageQueues() {
        List<MessageQueueImpl> messageQueues = new ArrayList<>();
        for (Partition partition : partitions) {
            messageQueues.add(new MessageQueueImpl(partition));
        }
        return messageQueues;
    }

    @VisibleForTesting
    public static ImmutableList<Partition> filterPartition(TopicRouteData topicRouteData) {
        final ImmutableList.Builder<Partition> builder = ImmutableList.builder();
        for (Partition partition : topicRouteData.getPartitions()) {
            if (!partition.getPermission().isWritable()) {
                continue;
            }
            if (MixAll.MASTER_BROKER_ID != partition.getBroker().getId()) {
                continue;
            }
            builder.add(partition);
        }
        final ImmutableList<Partition> partitions0 = builder.build();
        if (partitions0.isEmpty()) {
            LOGGER.warn("No available partition, topicRouteData={}", topicRouteData);
        }
        return partitions0;
    }

    public boolean isEmpty() {
        return partitions.isEmpty();
    }

    public List<Partition> takePartitions(Set<Endpoints> isolated, int count) {
        int nextIndex = index.getAndIncrement();
        List<Partition> candidatePartitions = new ArrayList<Partition>();
        Set<String> candidateBrokerNames = new HashSet<String>();
        // TODO: polish code
//        if (partitions.isEmpty()) {
//            throw new ClientException(ErrorCode.NO_PERMISSION);
//        }
        for (int i = 0; i < partitions.size(); i++) {
            final Partition partition = partitions.get(IntMath.mod(nextIndex++, partitions.size()));
            final Broker broker = partition.getBroker();
            final String brokerName = broker.getName();
            if (!isolated.contains(broker.getEndpoints()) && !candidateBrokerNames.contains(brokerName)) {
                candidateBrokerNames.add(brokerName);
                candidatePartitions.add(partition);
            }
            if (candidatePartitions.size() >= count) {
                return candidatePartitions;
            }
        }
        // if all endpoints are isolated.
        if (candidatePartitions.isEmpty()) {
            for (int i = 0; i < partitions.size(); i++) {
                final Partition partition = partitions.get(IntMath.mod(nextIndex++, partitions.size()));
                final Broker broker = partition.getBroker();
                final String brokerName = broker.getName();
                if (!candidateBrokerNames.contains(brokerName)) {
                    candidateBrokerNames.add(brokerName);
                    candidatePartitions.add(partition);
                }
                if (candidatePartitions.size() >= count) {
                    break;
                }
            }
        }
        return candidatePartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SendingTopicRouteData that = (SendingTopicRouteData) o;
        return Objects.equal(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(partitions);
    }
}
