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

package org.apache.rocketmq.grpcclient.impl.consumer;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import org.apache.rocketmq.apis.MessageQueue;
import org.apache.rocketmq.apis.message.MessageView;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @see ProcessQueue
 */
public class ProcessQueueImpl implements ProcessQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessQueueImpl.class);

    /**
     * Messages which is pending means have been cached, but are not taken by consumer dispatcher yet.
     */
    @GuardedBy("pendingMessagesLock")
    private final List<MessageView> pendingMessages;
    private final ReadWriteLock pendingMessagesLock;

    /**
     * Message which is in-flight means have been dispatched, but the consumption process is not accomplished.
     */
    @GuardedBy("inflightMessagesLock")
    private final List<MessageView> inflightMessages;
    private final ReadWriteLock inflightMessagesLock;

    private final AtomicLong cachedMessagesBytes;

    public ProcessQueueImpl() {
        this.pendingMessages = new ArrayList<>();
        this.pendingMessagesLock = new ReentrantReadWriteLock();
        this.inflightMessages = new ArrayList<>();
        this.inflightMessagesLock = new ReentrantReadWriteLock();
        this.cachedMessagesBytes = new AtomicLong();
    }

    @Override
    public MessageQueue getMessageQueue() {
        return null;
    }

    @Override
    public void drop() {
    }

    @Override
    public boolean expired() {
        return false;
    }

    public void cacheMessages(List<MessageView> messageList) {
        pendingMessagesLock.writeLock().lock();
        try {
            for (MessageView message : messageList) {
                pendingMessages.add(message);
                cachedMessagesBytes.addAndGet(message.getBody().length);
            }
        } finally {
            pendingMessagesLock.writeLock().unlock();
        }
    }

    @Override
    public void fetchMessageImmediately() {
    }

    @Override
    public List<MessageView> tryTakeMessages(int batchMaxSize) {
        pendingMessagesLock.writeLock().lock();
        inflightMessagesLock.writeLock().lock();
        try {
            final int actualSize = Math.min(pendingMessages.size(), batchMaxSize);
            final List<MessageView> subList = new ArrayList<>(pendingMessages.subList(0, actualSize));
            List<MessageView> messageExtList = new ArrayList<>(subList);
            inflightMessages.addAll(subList);
            pendingMessages.removeAll(subList);
            return messageExtList;
        } finally {
            inflightMessagesLock.writeLock().unlock();
            pendingMessagesLock.writeLock().lock();
        }
    }

    private void eraseMessages(List<MessageView> messageViews) {
        inflightMessagesLock.writeLock().lock();
        try {
            for (MessageView messageView : messageViews) {
                if (inflightMessages.remove(messageView)) {
                    cachedMessagesBytes.addAndGet(-messageView.getBody().length);
                }
            }
        } finally {
            inflightMessagesLock.writeLock().unlock();
        }
    }

    @Override
    public void eraseMessages(List<MessageView> messageList, ConsumeStatus status) {
    }

    @Override
    public Optional<MessageView> tryTakeFifoMessage() {
        return Optional.empty();
    }

    @Override
    public void eraseFifoMessage(MessageView message, ConsumeStatus status) {
    }

    @Override
    public void doStats() {

    }
}
