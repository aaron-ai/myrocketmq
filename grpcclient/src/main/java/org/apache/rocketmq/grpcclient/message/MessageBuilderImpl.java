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

import org.apache.rocketmq.apis.message.Message;
import org.apache.rocketmq.apis.message.MessageBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class MessageBuilderImpl implements MessageBuilder {
    private String topic = null;
    private byte[] body = null;
    private String tag = null;
    private String messageGroup = null;
    private Long deliveryTimestamp = null;
    private Collection<String> keys = new HashSet<>();
    private final Map<String, String> properties;

    public MessageBuilderImpl() {
        this.properties = new HashMap<>();
    }

    /**
     * See {@link MessageBuilder#setTopic(String)}
     */
    @Override
    public MessageBuilder setTopic(String topic) {
        this.topic = checkNotNull(topic, "topic should not be null");
        return this;
    }

    /**
     * See {@link MessageBuilder#setBody(byte[])}
     */
    @Override
    public MessageBuilder setBody(byte[] body) {
        checkNotNull(body, "body should not be null");
        this.body = body.clone();
        return this;
    }

    /**
     * See {@link MessageBuilder#setTag(String)}
     */
    @Override
    public MessageBuilder setTag(String tag) {
        this.tag = checkNotNull(tag, "tag should not be null");
        return this;
    }

    /**
     * See {@link MessageBuilder#setKeys(String...)}
     */
    @Override
    public MessageBuilder setKeys(String... keys) {
        checkNotNull(keys, "keys should not be null");
        this.keys = new ArrayList<>();
        this.keys.addAll(Arrays.asList(keys));
        return this;
    }

    /**
     * See {@link MessageBuilder#setMessageGroup(String)}
     */
    @Override
    public MessageBuilder setMessageGroup(String messageGroup) {
        checkArgument(null == deliveryTimestamp, "messageGroup and deliveryTimestamp should not be set at same time");
        this.messageGroup = checkNotNull(messageGroup, "messageGroup should not be null");
        return this;
    }

    /**
     * See {@link MessageBuilder#setDeliveryTimestamp(long)}
     */
    @Override
    public MessageBuilder setDeliveryTimestamp(long deliveryTimestamp) {
        checkArgument(null == messageGroup, "deliveryTimestamp and messageGroup should not be set at same time");
        this.deliveryTimestamp = deliveryTimestamp;
        return this;
    }

    /**
     * See {@link MessageBuilder#addProperty(String, String)}
     */
    @Override
    public MessageBuilder addProperty(String key, String value) {
        checkNotNull(key, "key should not be null");
        checkNotNull(value, "value should not be null");
        this.properties.put(key, value);
        return this;
    }

    /**
     * See {@link MessageBuilder#build()}
     */
    @Override
    public Message build() {
        checkNotNull(topic, "topic should not be null");
        checkNotNull(body, "body should not be null");
        return new MessageImpl(topic, body, tag, keys, messageGroup, deliveryTimestamp, properties);
    }
}
