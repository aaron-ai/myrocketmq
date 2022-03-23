package org.apache.rocketmq.grpcclient.message;

import org.apache.rocketmq.apis.MessageQueue;
import org.apache.rocketmq.apis.message.MessageId;
import org.apache.rocketmq.apis.message.MessageView;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public class MessageViewImpl implements MessageView {
    @Override
    public MessageId getMessageId() {
        return null;
    }

    @Override
    public String getTopic() {
        return null;
    }

    @Override
    public byte[] getBody() {
        return new byte[0];
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public Optional<String> getTag() {
        return Optional.empty();
    }

    @Override
    public Collection<String> getKeys() {
        return null;
    }

    @Override
    public Optional<String> getMessageGroup() {
        return Optional.empty();
    }

    @Override
    public Optional<Long> getDeliveryTimestamp() {
        return Optional.empty();
    }

    @Override
    public String getBornHost() {
        return null;
    }

    @Override
    public long getBornTimestamp() {
        return 0;
    }

    @Override
    public int getDeliveryAttempt() {
        return 0;
    }

    @Override
    public MessageQueue getMessageQueue() {
        return null;
    }

    @Override
    public long getOffset() {
        return 0;
    }
}
