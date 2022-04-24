package org.apache.rocketmq.grpcclient.impl;

public enum ClientType {
    PRODUCER,
    PUSH_CONSUMER,
    SIMPLE_CONSUMER;

    public apache.rocketmq.v2.ClientType toProtobuf() {
        if (PRODUCER.equals(this)) {
            return apache.rocketmq.v2.ClientType.PRODUCER;
        }
        if (PUSH_CONSUMER.equals(this)) {
            return apache.rocketmq.v2.ClientType.PUSH_CONSUMER;
        }
        if (SIMPLE_CONSUMER.equals(this)) {
            return apache.rocketmq.v2.ClientType.SIMPLE_CONSUMER;
        }
        return apache.rocketmq.v2.ClientType.CLIENT_TYPE_UNSPECIFIED;
    }
}
