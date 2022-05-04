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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.message.Message;
import org.apache.rocketmq.apis.producer.Transaction;
import org.apache.rocketmq.grpcclient.message.PublishingMessageImpl;
import org.apache.rocketmq.grpcclient.route.Endpoints;

public class TransactionImpl implements Transaction {

    private final ProducerSettings producerSettings;

    @GuardedBy("messagesLock")
    private final List<PublishingMessageImpl> messages;
    private final ReadWriteLock messagesLock;

    private final ConcurrentMap<PublishingMessageImpl, Endpoints> messageEndpointsMap;

    public TransactionImpl(ProducerSettings producerSettings) {
        this.producerSettings = producerSettings;
        this.messages = new ArrayList<>();
        this.messagesLock = new ReentrantReadWriteLock();
        this.messageEndpointsMap = new ConcurrentHashMap<>();
    }

    public Optional<PublishingMessageImpl> tryAddMessage(Message message) throws IOException {
        messagesLock.readLock().lock();
        try {
            if (!messages.isEmpty()) {
                return Optional.empty();
            }
        } finally {
            messagesLock.readLock().unlock();
        }
        messagesLock.writeLock().lock();
        try {
            if (!messages.isEmpty()) {
                return Optional.empty();
            }
            final PublishingMessageImpl publishingMessage = new PublishingMessageImpl(message, producerSettings, true);
            messages.add(publishingMessage);
            return Optional.of(publishingMessage);
        } finally {
            messagesLock.writeLock().unlock();
        }
    }

    @Override
    public void commit() throws ClientException {

    }

    @Override
    public void rollback() throws ClientException {

    }
}
