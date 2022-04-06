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
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.apis.MessageQueue;
import org.apache.rocketmq.grpcclient.message.MessageType;
import org.apache.rocketmq.grpcclient.message.protocol.Resource;

public class MessageQueueImpl implements MessageQueue {
    private final Resource topicResource;
    private final Broker broker;
    private final int queueId;

    private final Permission permission;
    private final List<MessageType> accept_message_types;

    public MessageQueueImpl(apache.rocketmq.v2.MessageQueue messageQueue) {
        final apache.rocketmq.v2.Resource resource = messageQueue.getTopic();
        this.topicResource = new Resource(resource.getResourceNamespace(), resource.getName());
        this.queueId = messageQueue.getId();
        final apache.rocketmq.v2.Permission perm = messageQueue.getPermission();
        this.permission = Permission.fromProto(perm);
        this.accept_message_types = new ArrayList<>();
        final List<apache.rocketmq.v2.MessageType> types = messageQueue.getAcceptMessageTypesList();
        for (apache.rocketmq.v2.MessageType type : types) {
            accept_message_types.add(MessageType.fromProtobuf(type));
        }
        final String brokerName = messageQueue.getBroker().getName();
        final int brokerId = messageQueue.getBroker().getId();
        final apache.rocketmq.v2.Endpoints endpoints = messageQueue.getBroker().getEndpoints();
        this.broker = new Broker(brokerName, brokerId, new Endpoints(endpoints));
    }

    public Resource getTopicResource() {
        return this.topicResource;
    }

    public Broker getBroker() {
        return this.broker;
    }

    @Override
    public String getTopic() {
        return topicResource.getName();
    }

    public int getQueueId() {
        return this.queueId;
    }

    public boolean matchMessageType(MessageType messageType) {
        return accept_message_types.contains(messageType);
    }

    //TODO
    @Override
    public String getId() {
        return null;
    }

    public Permission getPermission() {
        return this.permission;
    }

    // TODO
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MessageQueueImpl messageQueueImpl = (MessageQueueImpl) o;
        return queueId == messageQueueImpl.queueId && Objects.equal(topicResource, messageQueueImpl.topicResource) &&
            Objects.equal(broker, messageQueueImpl.broker) && permission == messageQueueImpl.permission;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(topicResource, broker, queueId, permission);
    }

    // TODO
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("topicResource", topicResource)
            .add("broker", broker)
            .add("id", queueId)
            .add("permission", permission)
            .toString();
    }
}
