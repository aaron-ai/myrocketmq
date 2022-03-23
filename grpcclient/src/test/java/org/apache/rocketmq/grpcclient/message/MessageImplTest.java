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

public class MessageImplTest {
    final ClientServiceProvider provider = ClientServiceProvider.loadService();

    @Test
    public void testMessageBodySetterGetterImmutability() {
        String sampleTopic = "foobar";
        byte[] sampleBody = new byte[]{'f', 'o', 'o'};

        final Message message = provider.newMessageBuilder().setTopic(sampleTopic).setBody(sampleBody).build();
        // Modify message body set before.
        sampleBody[0] = 'g';
        Assert.assertEquals('g', sampleBody[0]);
        Assert.assertEquals('f', message.getBody()[0]);

        final byte[] body = message.getBody();
        // Modify message body gotten before.
        body[0] = 'h';
        Assert.assertEquals('h', body[0]);
        Assert.assertEquals('f', message.getBody()[0]);
    }

    @Test
    public void testMessagePropertiesGetterImmutability() {
    }
}