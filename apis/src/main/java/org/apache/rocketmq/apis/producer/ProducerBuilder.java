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

package org.apache.rocketmq.apis.producer;

import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.message.Message;
import org.apache.rocketmq.apis.retry.BackoffRetryPolicy;

import java.util.List;

/**
 * Builder to config and start {@link Producer}.
 */
public interface ProducerBuilder {
    /**
     * Set the client configuration for producer.
     *
     * @param clientConfiguration client's configuration.
     * @return the producer builder instance.
     */
    ProducerBuilder setClientConfiguration(ClientConfiguration clientConfiguration);

    /**
     * Declare topics ahead of message sending.
     *
     * <p>Even though the declaration is not essential, we <strong>highly recommend</strong> to declare the topics in
     * advance, which could help to discover potential mistakes.
     *
     * @param topics topics to send/prepare.
     * @return the producer builder instance.
     */
    ProducerBuilder setTopics(String... topics);

    /**
     * Set the threads count for {@link Producer#sendAsync(Message)}.
     *
     * <p>Threads count is equals to CPU core number by default.
     *
     * @return the producer builder instance.
     */
    ProducerBuilder setSendAsyncThreadCount(int count);

    /**
     * Set the retry policy to send message.
     *
     * <p>Transactional message({@link Producer#send(Message, Transaction)}) doesn't apply retry policy.
     *
     * @param retryPolicy policy to re-send message when failure encountered.
     * @return the producer builder instance.
     */
    ProducerBuilder setRetryPolicy(BackoffRetryPolicy retryPolicy);

    /**
     * Set the transaction checker for producer.
     *
     * @param checker transaction checker.
     * @return the produce builder instance.
     */
    ProducerBuilder setTransactionChecker(TransactionChecker checker);

    /**
     * Finalize the build of {@link Producer} instance and start.
     *
     * <p>The producer does a series of preparatory work during startup, which could help to identify more unexpected
     * error earlier.
     *
     * <p>Especially, if this method is invoked more than once, different producer will be created and started.
     *
     * @return the producer instance.
     */
    Producer build() throws ClientException;
}
