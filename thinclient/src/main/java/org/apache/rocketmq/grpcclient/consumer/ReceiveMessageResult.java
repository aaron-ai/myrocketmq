package org.apache.rocketmq.grpcclient.consumer;

import apache.rocketmq.v2.Status;
import java.util.List;
import org.apache.rocketmq.apis.message.MessageView;
import org.apache.rocketmq.grpcclient.route.Endpoints;

public class ReceiveMessageResult {
    private final Endpoints endpoints;
    private final Status status;

    private final long receiveTimestamp;
    private final long invisibleDuration;

    private final List<MessageView> messagesFound;

    public ReceiveMessageResult(Endpoints endpoints, Status status, long receiveTimestamp, long invisibleDuration,
        List<MessageView> messagesFound) {
        this.endpoints = endpoints;
        this.status = status;
        this.receiveTimestamp = receiveTimestamp;
        this.invisibleDuration = invisibleDuration;
        this.messagesFound = messagesFound;
    }

    public Endpoints getEndpoints() {
        return endpoints;
    }

    public Status getStatus() {
        return status;
    }

    public long getReceiveTimestamp() {
        return receiveTimestamp;
    }

    public long getInvisibleDuration() {
        return invisibleDuration;
    }

    public List<MessageView> getMessagesFound() {
        return messagesFound;
    }
}
