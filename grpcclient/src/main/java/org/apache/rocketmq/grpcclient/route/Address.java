package org.apache.rocketmq.grpcclient.route;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class Address {
    private final String host;
    private final int port;

    public Address(apache.rocketmq.v1.Address address) {
        this.host = address.getHost();
        this.port = address.getPort();
    }

    public Address(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getAddress() {
        return host + ":" + port;
    }

    public apache.rocketmq.v1.Address toPbAddress() {
        return apache.rocketmq.v1.Address.newBuilder().setHost(host).setPort(port).build();
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Address address = (Address) o;
        return port == address.port && Objects.equal(host, address.host);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(host, port);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("host", host)
                .add("port", port)
                .toString();
    }
}
