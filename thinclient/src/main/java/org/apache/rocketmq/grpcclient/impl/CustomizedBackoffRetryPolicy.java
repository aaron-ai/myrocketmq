package org.apache.rocketmq.grpcclient.impl;

import com.google.common.base.MoreObjects;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.apis.retry.RetryPolicy;

import static com.google.common.base.Preconditions.checkArgument;

public class CustomizedBackoffRetryPolicy implements RetryPolicy {
    private final List<Duration> durations;
    private final int maxAttempts;

    public CustomizedBackoffRetryPolicy(List<Duration> durations, int maxAttempts) {
        checkArgument(!durations.isEmpty(), "durations must not be empty");
        this.durations = durations;
        this.maxAttempts = maxAttempts;
    }

    @Override
    public int getMaxAttempts() {
        return maxAttempts;
    }

    public List<Duration> getDurations() {
        return new ArrayList<>(durations);
    }

    @Override
    public Duration getNextAttemptDelay(int attempt) {
        checkArgument(attempt > 0, "attempt must be positive");
        return attempt > durations.size() ? durations.get(durations.size() - 1) : durations.get(attempt - 1);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("durations", durations)
            .add("maxAttempts", maxAttempts)
            .toString();
    }
}
