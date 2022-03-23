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

package org.apache.rocketmq.grpcclient.route;

import org.apache.rocketmq.apis.exception.ClientException;

import javax.annotation.concurrent.Immutable;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class TopicRouteDataResult {
    private final TopicRouteData topicRouteData;
    private final ClientException clientException;

    public TopicRouteDataResult(TopicRouteData topicRouteData) {
        this.topicRouteData = checkNotNull(topicRouteData, "topicRouteData should not be null");
        this.clientException = null;
    }

    public TopicRouteDataResult(ClientException clientException) {
        this.topicRouteData = null;
        this.clientException = checkNotNull(clientException, "clientException should not be null");
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public Optional<ClientException> tryGetClientException() {
        return null == clientException ? Optional.empty() : Optional.of(clientException);
    }
}
