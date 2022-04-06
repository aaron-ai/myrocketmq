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

import org.apache.rocketmq.apis.MessageQueue;
import org.apache.rocketmq.apis.message.MessageView;

import java.util.List;
import java.util.Optional;

/**
 * 1. Fetch 32 messages successfully from remote.
 * <pre>
 * 32 in   ┌─────────────────────────┐      ┌───┐
 * ────────►           32            │      │ 0 │
 *         └─────────────────────────┘      └───┘
 *               pending messages     in-flight messages
 * </pre>
 * 2. {@link #tryTakeMessages(int)} with 6 messages.
 * <pre>
 *         ┌─────────────────────┐  6   ┌───────────┐
 *         │         26          ├──────►     6     │ batch size=6
 *         └─────────────────────┘      └───────────┘
 *             pending messages      in-flight messages
 * </pre>
 * 3. {@link #eraseMessages(List, ConsumeStatus)} with 6 messages.
 * <pre>
 *         ┌─────────────────────┐      ┌───┐ 6 out
 *         │         26          │      │ 0 ├──────►
 *         └─────────────────────┘      └───┘
 *             pending messages      in-flight messages
 * </pre>
 * 4. {@link #tryTakeMessages(int)} with 6 messages.
 * <pre>
 *         ┌───────────────┐ 6  ┌───────────┐
 *         │       20      ├────►     6     ├──────►  batch size=6
 *         └───────────────┘    └───────────┘
 *          pending messages    in-flight messages
 * </pre>
 * 5. {@link #eraseMessages(List, ConsumeStatus)} with 6 messages.
 * <pre>
 *   1 in  ┌─────────────────┐      ┌───┐ 6 out
 *   ──────►        21       │      │ 0 ├──────►
 *         └─────────────────┘      └───┘
 *             pending messages  in-flight messages
 * </pre>
 */
public interface ProcessQueue {
    /**
     * Get the message queue bound.
     *
     * @return bound message queue.
     */
    MessageQueue getMessageQueue();

    /**
     * Drop current process queue, it would not fetch message from remote anymore if dropped.
     */
    void drop();

    /**
     * {@link ProcessQueue} would be regarded as expired if no fetch message for a long time.
     *
     * @return if it is expired.
     */
    boolean expired();

    /**
     * Start to fetch message from remote immediately.
     */
    void fetchMessageImmediately();

    /**
     * Try to take messages from cache except FIFO messages.
     *
     * @param batchMaxSize max batch size to take messages.
     * @return messages which have been taken.
     */
    List<MessageView> tryTakeMessages(int batchMaxSize);

    /**
     * Erase messages which haven been taken except FIFO messages.
     *
     * @param messageList messages to erase.
     * @param status      consume status.
     */
    void eraseMessages(List<MessageView> messageList, ConsumeStatus status);

    /**
     * Try to take a FIFO message from cache.
     *
     * @return message which has been taken, or {@link Optional#empty()} ()} if no message.
     */
    Optional<MessageView> tryTakeFifoMessage();

    /**
     * Erase FIFO message which has been taken.
     *
     * @param message message to erase.
     * @param status  consume status.
     */
    void eraseFifoMessage(MessageView message, ConsumeStatus status);

    /**
     * Do some stats.
     */
    void doStats();
}
