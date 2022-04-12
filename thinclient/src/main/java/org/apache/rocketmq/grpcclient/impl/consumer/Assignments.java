package org.apache.rocketmq.grpcclient.impl.consumer;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.util.List;

public class Assignments {
    private final List<Assignment> assignmentList;

    public Assignments(List<Assignment> assignmentList) {
        this.assignmentList = assignmentList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Assignments that = (Assignments) o;
        return Objects.equal(assignmentList, that.assignmentList);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(assignmentList);
    }

    @Override public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("assignmentList", assignmentList)
            .toString();
    }

    public List<Assignment> getAssignmentList() {
        return assignmentList;
    }
}
