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

import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Status;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import io.grpc.Metadata;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.message.MessageView;
import org.apache.rocketmq.grpcclient.consumer.ReceiveMessageResult;
import org.apache.rocketmq.grpcclient.impl.ClientImpl;
import org.apache.rocketmq.grpcclient.message.MessageViewImpl;
import org.apache.rocketmq.grpcclient.route.Endpoints;
import org.apache.rocketmq.grpcclient.route.MessageQueueImpl;

@SuppressWarnings("UnstableApiUsage")
public abstract class ConsumerImpl extends ClientImpl {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerImpl.class);

    private final String consumerGroup;

    ConsumerImpl(ClientConfiguration clientConfiguration, String consumerGroup, Set<String> topics) {
        super(clientConfiguration, topics);
        this.consumerGroup = consumerGroup;
    }

    @SuppressWarnings("SameParameterValue")
    protected ListenableFuture<ReceiveMessageResult> receiveMessage(ReceiveMessageRequest request, MessageQueueImpl mq,
        Duration timeout) {
        List<MessageView> messages = new ArrayList<>();
        final SettableFuture<ReceiveMessageResult> future0 = SettableFuture.create();
        try {
            Metadata metadata = sign();
            final Endpoints endpoints = mq.getBroker().getEndpoints();
            final ListenableFuture<Iterator<ReceiveMessageResponse>> future = clientManager.receiveMessage(endpoints, metadata, request, timeout);
            return Futures.transform(future, it -> {
                // Null here means status not set yet.
                Status status = null;
                while (it.hasNext()) {
                    final ReceiveMessageResponse response = it.next();
                    switch (response.getContentCase()) {
                        case STATUS:
                            status = response.getStatus();
                            break;
                        case MESSAGE:
                            final Message message = response.getMessage();
                            final MessageViewImpl messageView = MessageViewImpl.fromProtobuf(message, mq);
                            messages.add(messageView);
                            break;
                        default:
                            LOGGER.warn("[Bug] Not recognized content for receive message response, mq={}, resp={}", mq, response);
                    }
                }
                return new ReceiveMessageResult(endpoints, status, messages);
            }, MoreExecutors.directExecutor());
        } catch (Throwable t) {
            future0.setException(t);
            return future0;
        }
    }

    @Override
    public HeartbeatRequest wrapHeartbeatRequest() {
        return HeartbeatRequest.newBuilder().setGroup(getProtobufGroup()).build();
    }

    protected Resource getProtobufGroup() {
        return Resource.newBuilder().setName(consumerGroup).build();
    }

    @Override
    public NotifyClientTerminationRequest wrapNotifyClientTerminationRequest() {
        return NotifyClientTerminationRequest.newBuilder().setGroup(getProtobufGroup()).build();
    }
}
