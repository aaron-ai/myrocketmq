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

package org.apache.rocketmq.grpcclient.message;

import org.apache.rocketmq.apis.MessageQueue;
import org.apache.rocketmq.apis.message.MessageId;
import org.apache.rocketmq.apis.message.MessageView;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class MessageViewImpl implements MessageView {
    private final MessageId messageId;
    private final String topic;
    private final byte[] body;
    private final String tag;
    private final String messageGroup;
    private final Long deliveryTimestamp;
    private final Collection<String> keys;
    private final Map<String, String> properties;
    private final String bornHost;
    private final long bornTimestamp;
    private final int deliveryAttempt;
    private final MessageQueue messageQueue;
    private final long offset;
    private final boolean corrupted;

    public MessageViewImpl(MessageId messageId, String topic, byte[] body, String tag, String messageGroup,
                           Long deliveryTimestamp, Collection<String> keys, Map<String, String> properties,
                           String bornHost, long bornTimestamp, int deliveryAttempt, MessageQueue messageQueue,
                           long offset, boolean corrupted) {
        this.messageId = checkNotNull(messageId, "messageId should not be null");
        this.topic = checkNotNull(topic, "topic should not be null");
        this.body = checkNotNull(body, "body should not be null");
        this.tag = tag;
        this.messageGroup = messageGroup;
        this.deliveryTimestamp = deliveryTimestamp;
        this.keys = checkNotNull(keys, "keys should not be null");
        this.properties = checkNotNull(properties, "properties should not be null");
        this.bornHost = checkNotNull(bornHost, "bornHost should not be null");
        this.bornTimestamp = bornTimestamp;
        this.deliveryAttempt = deliveryAttempt;
        this.messageQueue = checkNotNull(messageQueue, "messageQueue should not be null");
        this.offset = offset;
        this.corrupted = corrupted;
    }

    /**
     * @see MessageView#getMessageId()
     */
    @Override
    public MessageId getMessageId() {
        return messageId;
    }

    /**
     * @see MessageView#getTopic()
     */
    @Override
    public String getTopic() {
        return topic;
    }

    /**
     * @see MessageView#getBody()
     */
    @Override
    public byte[] getBody() {
        return body.clone();
    }

    /**
     * @see MessageView#getProperties()
     */
    @Override
    public Map<String, String> getProperties() {
        return new HashMap<>(properties);
    }

    /**
     * @see MessageView#getTag()
     */
    @Override
    public Optional<String> getTag() {
        return null == tag ? Optional.empty() : Optional.of(tag);
    }

    /**
     * @see MessageView#getKeys()
     */
    @Override
    public Collection<String> getKeys() {
        return keys;
    }

    /**
     * @see MessageView#getMessageGroup()
     */
    @Override
    public Optional<String> getMessageGroup() {
        return null == messageGroup ? Optional.empty() : Optional.of(messageGroup);
    }

    /**
     * @see MessageView#getDeliveryTimestamp()
     */
    @Override
    public Optional<Long> getDeliveryTimestamp() {
        return null == deliveryTimestamp ? Optional.empty() : Optional.of(deliveryTimestamp);
    }

    /**
     * @see MessageView#getBornHost()
     */
    @Override
    public String getBornHost() {
        return bornHost;
    }

    /**
     * @see MessageView#getBornTimestamp()
     */
    @Override
    public long getBornTimestamp() {
        return bornTimestamp;
    }

    /**
     * @see MessageView#getDeliveryAttempt()
     */
    @Override
    public int getDeliveryAttempt() {
        return deliveryAttempt;
    }

    /**
     * @see MessageView#getMessageQueue()
     */
    @Override
    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    /**
     * @see MessageView#getOffset()
     */
    @Override
    public long getOffset() {
        return offset;
    }

    public boolean isCorrupted() {
        return corrupted;
    }
}
