package org.apache.rocketmq.grpcclient.tool;

import apache.rocketmq.v2.Address;
import apache.rocketmq.v2.AddressScheme;

public class TestBase {
    protected static final String FAKE_TOPIC_0 = "foo-bar-topic-0";
    protected static final String FAKE_BROKER_NAME_0 = "foo-bar-broker-name-0";

    protected static final String FAKE_ACCESS_POINT = "127.0.0.1:9876";
    protected static final String FAKE_HOST_0 = "127.0.0.1";
    protected static final int FAKE_PORT_0 = 8080;

    protected Address fakePbAddress0() {
        return fakePbAddress(FAKE_HOST_0, FAKE_PORT_0);
    }

    protected Address fakePbAddress(String host, int port) {
        return Address.newBuilder().setHost(host).setPort(port).build();
    }

    protected apache.rocketmq.v2.Endpoints fakePbEndpoints0() {
        return fakePbEndpoints(fakePbAddress0());
    }

    protected apache.rocketmq.v2.Endpoints fakePbEndpoints(Address address) {
        return apache.rocketmq.v2.Endpoints.newBuilder().setScheme(AddressScheme.IPv4).addAddresses(address).build();
    }
}
