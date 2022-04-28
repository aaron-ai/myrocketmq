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

package org.apache.rocketmq.apis.retry;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;

import java.time.Duration;
import java.util.Random;

/**
 * The {@link ExponentialBackoffRetryPolicy} defines a policy to do more attempts when failure is encountered, mainly refer to
 * <a href="https://github.com/grpc/proposal/blob/master/A6-client-retries.md">gRPC Retry Design</a>.
 */
public class ExponentialBackoffRetryPolicy implements RetryPolicy {
    public static BackOffRetryPolicyBuilder newBuilder() {
        return new BackOffRetryPolicyBuilder();
    }

    private final Random random;
    private final int maxAttempts;
    private final Duration initialBackoff;
    private final Duration maxBackoff;
    private final double backoffMultiplier;

    /**
     * The caller is supposed to have validated the arguments and handled throwing exception or
     * logging warnings already, so we avoid repeating args check here.
     */
    public ExponentialBackoffRetryPolicy(int maxAttempts, Duration initialBackoff, Duration maxBackoff,
        double backoffMultiplier) {
        this.random = new Random();
        this.maxAttempts = maxAttempts;
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
        this.backoffMultiplier = backoffMultiplier;
    }

    @Override
    public int getMaxAttempts() {
        return maxAttempts;
    }

    @Override
    public Duration getNextAttemptDelay(int attempt) {
        checkArgument(attempt > 0, "attempt must be positive");
        int randomNumberBound = (int) Math.min(initialBackoff.getNano() * Math.pow(backoffMultiplier, 1.0 * (attempt - 1)),
            maxBackoff.getNano());
        return Duration.ofNanos(random.nextInt(randomNumberBound));
    }

    public Duration getInitialBackoff() {
        return initialBackoff;
    }

    public Duration getMaxBackoff() {
        return maxBackoff;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("maxAttempts", maxAttempts)
            .add("initialBackoff", initialBackoff)
            .add("maxBackoff", maxBackoff)
            .add("backoffMultiplier", backoffMultiplier)
            .toString();
    }
}
