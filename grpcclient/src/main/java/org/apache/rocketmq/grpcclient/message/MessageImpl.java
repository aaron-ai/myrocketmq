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

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public class MessageImpl implements Message {
    public MessageImpl() {
    }

    @Override
    public String getTopic() {
        return null;
    }

    @Override
    public byte[] getBody() {
        return new byte[0];
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public Optional<String> getTag() {
        return Optional.empty();
    }

    @Override
    public Collection<String> getKeys() {
        return null;
    }

    @Override
    public Optional<String> getMessageGroup() {
        return Optional.empty();
    }

    @Override
    public Optional<Long> getDeliveryTimestamp() {
        return Optional.empty();
    }
}
