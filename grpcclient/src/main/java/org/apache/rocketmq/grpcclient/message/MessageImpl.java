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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

class MessageImpl implements Message {
    private final String topic;
    private final byte[] body;
    private final String tag;
    private final String messageGroup;
    private final Long deliveryTimestamp;
    private final Collection<String> keys;
    private final Map<String, String> properties;

    /**
     * The caller is supposed to have validated the arguments and handled throwing exception or
     * logging warnings already, so we avoid repeating args check here.
     */
    MessageImpl(String topic, byte[] body, String tag, Collection<String> keys, String messageGroup,
                Long deliveryTimestamp, Map<String, String> properties) {
        this.topic = topic;
        this.body = body;
        this.tag = tag;
        this.messageGroup = messageGroup;
        this.deliveryTimestamp = deliveryTimestamp;
        this.keys = keys;
        this.properties = properties;
    }

    /**
     * See {@link Message#getTopic()}
     */
    @Override
    public String getTopic() {
        return topic;
    }

    /**
     * See {@link Message#getBody()}
     */
    @Override
    public byte[] getBody() {
        return body;
    }

    /**
     * See {@link Message#getProperties()}
     */
    @Override
    public Map<String, String> getProperties() {
        return new HashMap<>(properties);
    }

    /**
     * See {@link Message#getTag()}
     */
    @Override
    public Optional<String> getTag() {
        return null == tag ? Optional.empty() : Optional.of(tag);
    }

    @Override
    public Collection<String> getKeys() {
        return new ArrayList<>(keys);
    }

    @Override
    public Optional<Long> getDeliveryTimestamp() {
        return null == deliveryTimestamp ? Optional.empty() : Optional.of(deliveryTimestamp);
    }

    @Override
    public Optional<String> getMessageGroup() {
        return null == messageGroup ? Optional.empty() : Optional.of(messageGroup);
    }
}
