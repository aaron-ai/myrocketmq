package org.apache.rocketmq.grpcclient.impl.consumer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Status;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
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

    ConsumerImpl(ClientConfiguration clientConfiguration, Set<String> topics) {
        super(clientConfiguration, topics);
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
}
