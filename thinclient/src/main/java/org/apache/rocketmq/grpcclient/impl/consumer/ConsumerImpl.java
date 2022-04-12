package org.apache.rocketmq.grpcclient.impl.consumer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Status;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import io.grpc.Metadata;
import java.time.Duration;
import java.util.ArrayList;
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
    protected ListenableFuture<ReceiveMessageResult> receiveMessage(ReceiveMessageRequest request,
        MessageQueueImpl messageQueue, Duration timeout) {
        final SettableFuture<ReceiveMessageResult> future0 = SettableFuture.create();
        try {
            Metadata metadata = sign();
            final Endpoints endpoints = messageQueue.getBroker().getEndpoints();
            final ListenableFuture<ReceiveMessageResponse> future =
                clientManager.receiveMessage(endpoints, metadata, request, timeout);
            return Futures.transform(future, (Function<ReceiveMessageResponse, ReceiveMessageResult>)
                response -> processReceiveMessageResponse(messageQueue, response));
        } catch (Throwable t) {
            future0.setException(t);
            return future0;
        }
    }

    public ReceiveMessageResult processReceiveMessageResponse(MessageQueueImpl mq, ReceiveMessageResponse response) {
        final Endpoints endpoints = mq.getBroker().getEndpoints();
        final Status status = response.getStatus();
        final Code code = status.getCode();
        switch (code) {
            case OK:
                break;
            // Fall through on purpose.
            case UNAUTHORIZED:
            case FORBIDDEN:
            case TOO_MANY_REQUESTS:
            default:
                LOGGER.error("Failed to receive message from remote, clientId={}, code={}, mq={}," +
                    " status message=[{}]", clientId, code, mq, status.getMessage());

        }
        List<MessageView> messages = new ArrayList<>();
        if (Code.OK.equals(code)) {
            final List<Message> messageList = response.getMessagesList();
            for (Message message : messageList) {
                final MessageViewImpl messageView = MessageViewImpl.fromProtobuf(message, mq);
                messages.add(messageView);
            }
        }
        return new ReceiveMessageResult(endpoints, status, Timestamps.toMillis(response.getDeliveryTimestamp()),
            Durations.toMillis(response.getInvisibleDuration()), messages);
    }
}
