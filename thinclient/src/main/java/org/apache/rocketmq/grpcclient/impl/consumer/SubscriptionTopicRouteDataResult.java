package org.apache.rocketmq.grpcclient.impl.consumer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Status;
import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.Immutable;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.apis.exception.AuthenticationException;
import org.apache.rocketmq.apis.exception.AuthorisationException;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.exception.ResourceNotFoundException;
import org.apache.rocketmq.grpcclient.route.MessageQueueImpl;
import org.apache.rocketmq.grpcclient.route.TopicRouteDataResult;
import org.apache.rocketmq.grpcclient.utility.MixAll;

@Immutable
public class SubscriptionTopicRouteDataResult {
    private final AtomicInteger messageQueueIndex;

    private final Status status;

    private final ImmutableList<MessageQueueImpl> messageQueues;

    public SubscriptionTopicRouteDataResult(TopicRouteDataResult topicRouteDataResult) {
        this.messageQueueIndex = new AtomicInteger(RandomUtils.nextInt(0, Integer.MAX_VALUE));
        this.status = topicRouteDataResult.getStatus();
        final ImmutableList.Builder<MessageQueueImpl> builder = ImmutableList.builder();
        if (Code.OK != status.getCode()) {
            this.messageQueues = builder.build();
            return;
        }
        for (MessageQueueImpl messageQueue : topicRouteDataResult.getTopicRouteData().getMessageQueues()) {
            if (!messageQueue.getPermission().isReadable() ||
                MixAll.MASTER_BROKER_ID != messageQueue.getBroker().getId()) {
                continue;
            }
            builder.add(messageQueue);
        }
        this.messageQueues = builder.build();
    }

    public MessageQueueImpl takeMessageQueue() throws ClientException {
        final Code code = status.getCode();
        switch (code) {
            case OK:
                break;
            case FORBIDDEN:
                throw new AuthorisationException(code.getNumber(), status.getMessage());
            case UNAUTHORIZED:
                throw new AuthenticationException(code.getNumber(), status.getMessage());
            case TOPIC_NOT_FOUND:
                throw new ResourceNotFoundException(code.getNumber(), status.getMessage());
            case ILLEGAL_ACCESS_POINT:
                throw new IllegalArgumentException("Access point is illegal");
        }
        if (messageQueues.isEmpty()) {
            // TODO
            throw new AuthorisationException("Readable message queue is empty");
        }
        final int next = messageQueueIndex.getAndIncrement();
        return messageQueues.get(IntMath.mod(next, messageQueues.size()));
    }
}
