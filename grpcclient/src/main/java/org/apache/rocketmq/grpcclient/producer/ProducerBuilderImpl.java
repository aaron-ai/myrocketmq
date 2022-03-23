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

package org.apache.rocketmq.grpcclient.producer;

import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.producer.Producer;
import org.apache.rocketmq.apis.producer.ProducerBuilder;
import org.apache.rocketmq.apis.producer.TransactionChecker;
import org.apache.rocketmq.apis.retry.BackoffRetryPolicy;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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

    @Override
    public ProducerBuilder setClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = checkNotNull(clientConfiguration, "clientConfiguration should not be null");
        return this;
    }

    @Override
    public ProducerBuilder setTopics(String... topics) {
        this.topics.addAll(Arrays.asList(topics));
        return this;
    }

    @Override
    public ProducerBuilder setSendAsyncThreadCount(int count) {
        checkArgument(count > 0, "producer send async thread count should be positive");
        this.asyncThreadCount = count;
        return this;
    }

    @Override
    public ProducerBuilder setRetryPolicy(BackoffRetryPolicy retryPolicy) {
        this.retryPolicy = checkNotNull(retryPolicy, "retryPolicy should not be null");
        return this;
    }

    @Override
    public ProducerBuilder setTransactionChecker(TransactionChecker checker) {
        this.checker = checkNotNull(checker, "checker should not set null");
        return this;
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public Producer build() {
        checkNotNull(topics, "topics should not be null");
        checkNotNull(retryPolicy, "retryPolicy should not be null");
        checkNotNull(checker, "checker should not be null");
        final ProducerImpl producer = new ProducerImpl(clientConfiguration, topics, asyncThreadCount, retryPolicy,
                checker);
        producer.startAsync().awaitRunning();
        return producer;
    }
}
