package org.apache.rocketmq.grpcclient.impl.consumer;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.rocketmq.grpcclient.route.MessageQueueImpl;

public class Assignment {
    private final MessageQueueImpl messageQueue;

    public Assignment(MessageQueueImpl messageQueue) {
        this.messageQueue = messageQueue;
    }

    public MessageQueueImpl getMessageQueue() {
        return messageQueue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Assignment that = (Assignment) o;
        return Objects.equal(messageQueue, that.messageQueue);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(messageQueue);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("messageQueue", messageQueue)
            .toString();
    }
}
