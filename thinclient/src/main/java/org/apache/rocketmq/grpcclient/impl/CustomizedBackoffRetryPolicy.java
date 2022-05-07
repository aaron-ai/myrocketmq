/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
