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

import java.time.Duration;

public class BackOffRetryPolicyBuilder {
    private int maxAttempts;
    private Duration initialBackoff;
    private Duration maxBackoff;
    private int backoffMultiplier;

    public BackOffRetryPolicyBuilder() {
        this.maxAttempts = 3;
        this.initialBackoff = Duration.ofMillis(100);
        this.maxBackoff = Duration.ofSeconds(1);
        this.backoffMultiplier = 2;
    }

    public BackOffRetryPolicyBuilder setMaxAttempts(int maxAttempts) {
        checkArgument(maxAttempts > 0, "maxAttempts must be positive");
        this.maxAttempts = maxAttempts;
        return this;
    }

    public BackOffRetryPolicyBuilder setInitialBackoff(Duration initialBackoff) {
        this.initialBackoff = checkNotNull(initialBackoff, "initialBackoff should not be null");
        return this;
    }

    public BackOffRetryPolicyBuilder setMaxBackoff(Duration maxBackoff) {
        this.maxBackoff = checkNotNull(maxBackoff, "maxBackoff should not be null");
        return this;
    }

    public BackOffRetryPolicyBuilder setBackoffMultiplier(int backoffMultiplier) {
        checkArgument(backoffMultiplier > 0, "backoffMultiplier must be positive");
        this.backoffMultiplier = backoffMultiplier;
        return this;
    }

    public ExponentialBackoffRetryPolicy build() {
        checkArgument(maxBackoff.compareTo(initialBackoff) >= 0, "initialBackoff should not be minor than maxBackoff");
        checkArgument(maxAttempts > 0, "maxAttempts must be positive");
        checkNotNull(initialBackoff, "initialBackoff should not be null");
        return new ExponentialBackoffRetryPolicy(maxAttempts, initialBackoff, maxBackoff, backoffMultiplier);
    }
}
