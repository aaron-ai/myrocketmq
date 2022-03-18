package org.apache.rocketmq.grpcclient.route;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import javax.annotation.concurrent.Immutable;

@Immutable
public class Broker {
    private final String name;
    private final int id;
    private final Endpoints endpoints;

    public Broker(String name, int id, Endpoints endpoints) {
        this.name = name;
        this.id = id;
        this.endpoints = endpoints;
    }

    public String getName() {
        return this.name;
    }

    public int getId() {
        return this.id;
    }

    public Endpoints getEndpoints() {
        return this.endpoints;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Broker broker = (Broker) o;
        return id == broker.id && Objects.equal(name, broker.name) && Objects.equal(endpoints, broker.endpoints);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, id, endpoints);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("id", id)
                .add("endpoints", endpoints)
                .toString();
    }
}