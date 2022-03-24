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

import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.grpcclient.route.TopicRouteDataResult;
import org.apache.rocketmq.grpcclient.utility.UtilAll;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkNotNull;

@SuppressWarnings("UnstableApiUsage")
public class ClientImpl extends AbstractIdleService implements Client {

    private final ClientConfiguration clientConfiguration;
    private final ConcurrentMap<String, TopicRouteDataResult> topicRouteCache;
    protected final String clientId;

    public ClientImpl(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = checkNotNull(clientConfiguration, "clientConfiguration should not be null");
        this.topicRouteCache = new ConcurrentHashMap<>();
        this.clientId = UtilAll.genClientId();
    }

    @Override
    protected void startUp() {

    }

    @Override
    protected void shutDown() throws InterruptedException {

    }

    @Override
    public String getClientId() {
        return clientId;
    }

    @Override
    public void doHeartbeat() {

    }

    @Override
    public void doHealthCheck() {

    }

    @Override
    public void doStats() {

    }
}
