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

import org.apache.rocketmq.apis.ClientServiceProvider;
import org.apache.rocketmq.apis.message.Message;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MessageImplTest {
    private final ClientServiceProvider provider = ClientServiceProvider.loadService();
    private final String sampleTopic = "foobar";
    private final byte[] sampleBody = new byte[]{'f', 'o', 'o'};

    @Test(expected = NullPointerException.class)
    public void testTopicSetterWithNull() {
        provider.newMessageBuilder().setTopic(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTopicSetterWithLengthEquals128() {
        String topicWithLengthEquals128 = new String(new char[128]).replace("\0", "a");
        provider.newMessageBuilder().setTopic(topicWithLengthEquals128);
    }

    @Test
    public void testMessageBodySetterGetterImmutability() {
        byte[] body = sampleBody.clone();

        final Message message = provider.newMessageBuilder().setTopic(sampleTopic).setBody(body).build();
        // Modify message body set before.
        body[0] = 'g';
        Assert.assertEquals('g', body[0]);
        Assert.assertEquals('f', message.getBody()[0]);

        final byte[] bodyGotten = message.getBody();
        // Modify message body gotten before.
        bodyGotten[0] = 'h';
        Assert.assertEquals('h', bodyGotten[0]);
        Assert.assertEquals('f', message.getBody()[0]);
    }

    @Test
    public void testMessagePropertiesGetterImmutability() {
        byte[] body = sampleBody.clone();

        String propertyKey = "foo";
        String propertyValue = "value";
        Map<String, String> property = new HashMap<>();
        property.put(propertyKey, propertyValue);

        final Message message =
                provider.newMessageBuilder().setTopic(sampleTopic).setBody(body).addProperty(propertyKey,
                        propertyValue).build();
        Assert.assertEquals(property, message.getProperties());
        // Clear properties gotten.
        message.getProperties().clear();
        Assert.assertEquals(property, message.getProperties());
    }
}