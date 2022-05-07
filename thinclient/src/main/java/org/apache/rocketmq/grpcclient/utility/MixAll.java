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

package org.apache.rocketmq.grpcclient.utility;

import apache.rocketmq.v2.ReceiveMessageRequest;

public class MixAll {
    @SuppressWarnings("HttpUrlsUsage")
    public static final String HTTP_PREFIX = "http://";
    public static final String HTTPS_PREFIX = "https://";

    public static final int MASTER_BROKER_ID = 0;

    private static String protocolVersion = null;

    private MixAll() {
    }

    public static String getProtocolVersion() {
        if (null != protocolVersion) {
            return protocolVersion;
        }
        protocolVersion = ReceiveMessageRequest.class.getName().split("\\.")[2];
        return protocolVersion;
    }
}
