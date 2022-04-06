package org.apache.rocketmq.grpcclient.impl.producer;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.producer.Transaction;
import org.apache.rocketmq.grpcclient.message.PublishingMessageImpl;

public class TransactionImpl implements Transaction {
    private final List<PublishingMessageImpl> messageList;

    public TransactionImpl() {
        this.messageList = new ArrayList<>();
    }

    public void addMessage(PublishingMessageImpl messageImpl) {
        this.messageList.add(messageImpl);
    }

    @Override
    public void commit() throws ClientException {

    }

    @Override
    public void rollback() throws ClientException {

    }
}
