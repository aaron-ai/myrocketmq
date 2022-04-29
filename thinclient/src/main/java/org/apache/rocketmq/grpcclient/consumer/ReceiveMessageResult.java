package org.apache.rocketmq.grpcclient.consumer;

import apache.rocketmq.v2.Status;
import java.util.List;
import java.util.Optional;
import org.apache.rocketmq.apis.message.MessageView;
import org.apache.rocketmq.grpcclient.message.MessageViewImpl;
import org.apache.rocketmq.grpcclient.route.Endpoints;

public class ReceiveMessageResult {
    private final Endpoints endpoints;
    private final Status status;

    private final List<MessageViewImpl> messages;

    public ReceiveMessageResult(Endpoints endpoints, Status status, List<MessageViewImpl> messages) {
        this.endpoints = endpoints;
        this.status = status;
        this.messages = messages;
    }

    public Endpoints getEndpoints() {
        return endpoints;
    }

    public Optional<Status> getStatus() {
        return null == status ? Optional.empty() : Optional.of(status);
    }

    public List<MessageViewImpl> getMessages() {
        return messages;
    }
}
