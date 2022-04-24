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

import java.util.stream.Collectors;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.producer.Producer;
import org.apache.rocketmq.apis.producer.ProducerBuilder;
import org.apache.rocketmq.apis.producer.TransactionChecker;
import org.apache.rocketmq.apis.retry.BackoffRetryPolicy;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.grpcclient.message.MessageBuilderImpl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ProducerBuilderImpl implements ProducerBuilder {
    private ClientConfiguration clientConfiguration = null;
    private final Set<String> topics = new HashSet<>();
    private int asyncThreadCount = Runtime.getRuntime().availableProcessors();
    private BackoffRetryPolicy retryPolicy = BackoffRetryPolicy.newBuilder().build();
    private TransactionChecker checker = null;

    public ProducerBuilderImpl() {
    }

    /**
     * @see ProducerBuilder#setClientConfiguration(ClientConfiguration)
     */
    @Override
    public ProducerBuilder setClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = checkNotNull(clientConfiguration, "clientConfiguration should not be null");
        return this;
    }

    /**
     * @see ProducerBuilder#setTopics(String...)
     */
    @Override
    public ProducerBuilder setTopics(String... topics) {
        final Set<String> set = Arrays.stream(topics).peek(topic -> {
            checkNotNull(topic, "topic should not be null");
            checkArgument(MessageBuilderImpl.TOPIC_PATTERN.matcher(topic).matches(), "topic does not match the regex [regex=%s]",
                MessageBuilderImpl.TOPIC_PATTERN.pattern());
        }).collect(Collectors.toSet());
        this.topics.addAll(set);
        return this;
    }

    /**
     * @see ProducerBuilder#setSendAsyncThreadCount(int)
     */
    @Override
    public ProducerBuilder setSendAsyncThreadCount(int count) {
        checkArgument(count > 0, "producer send async thread count should be positive");
        this.asyncThreadCount = count;
        return this;
    }

    /**
     * @see ProducerBuilder#setRetryPolicy(BackoffRetryPolicy)
     */
    @Override
    public ProducerBuilder setRetryPolicy(BackoffRetryPolicy retryPolicy) {
        this.retryPolicy = checkNotNull(retryPolicy, "retryPolicy should not be null");
        return this;
    }

    /**
     * @see ProducerBuilder#setTransactionChecker(TransactionChecker)
     */
    @Override
    public ProducerBuilder setTransactionChecker(TransactionChecker checker) {
        this.checker = checkNotNull(checker, "checker should not set null");
        return this;
    }

    /**
     * @see ProducerBuilder#build()
     */
    @Override
    public Producer build() {
        checkNotNull(clientConfiguration, "clientConfiguration has not been set yet");
        return new ProducerImpl(clientConfiguration, topics, asyncThreadCount, retryPolicy, checker);
    }
}
