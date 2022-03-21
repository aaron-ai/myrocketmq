package org.apache.rocketmq.grpcclient.route;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.rocketmq.grpcclient.message.protocol.Resource;

public class Partition {
    private final Resource topicResource;
    private final Broker broker;
    private final int id;

    private final Permission permission;

    public Partition(apache.rocketmq.v1.Partition partition) {
        final apache.rocketmq.v1.Resource resource = partition.getTopic();
        this.topicResource = new Resource(resource.getResourceNamespace(), resource.getName());
        this.id = partition.getId();
        final apache.rocketmq.v1.Permission perm = partition.getPermission();
        switch (perm) {
            case READ:
                this.permission = Permission.READ;
                break;
            case WRITE:
                this.permission = Permission.WRITE;
                break;
            case READ_WRITE:
                this.permission = Permission.READ_WRITE;
                break;
            case NONE:
            default:
                this.permission = Permission.NONE;
                break;
        }

        final String brokerName = partition.getBroker().getName();
        final int brokerId = partition.getBroker().getId();

        final apache.rocketmq.v1.Endpoints endpoints = partition.getBroker().getEndpoints();
        this.broker = new Broker(brokerName, brokerId, new Endpoints(endpoints));
    }

    public Resource getTopicResource() {
        return this.topicResource;
    }

    public Broker getBroker() {
        return this.broker;
    }

    public int getId() {
        return this.id;
    }

    public Permission getPermission() {
        return this.permission;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Partition partition = (Partition) o;
        return id == partition.id && Objects.equal(topicResource, partition.topicResource) &&
                Objects.equal(broker, partition.broker) && permission == partition.permission;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(topicResource, broker, id, permission);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("topicResource", topicResource)
                .add("broker", broker)
                .add("id", id)
                .add("permission", permission)
                .toString();
    }
}