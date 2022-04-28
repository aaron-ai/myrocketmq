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

package org.apache.rocketmq.grpcclient.impl.producer;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.producer.Transaction;
import org.apache.rocketmq.grpcclient.message.PublishingMessageImpl;

public class TransactionImpl implements Transaction {
    private final List<PublishingMessageImpl> messageList;

    public TransactionImpl() {
        this.messageList = new ArrayList<>();
    }

    public void addMessage(PublishingMessageImpl messageImpl) {
        this.messageList.add(messageImpl);
    }

    @Override
    public void commit() throws ClientException {

    }

    @Override
    public void rollback() throws ClientException {

    }
}
