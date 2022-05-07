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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@ThreadSafe
public class ClientManagerRegistry {
    @GuardedBy("clientIdsLock")
    private static final Set<String> CLIENT_IDS = new HashSet<>();
    private static final Lock CLIENT_IDS_LOCK = new ReentrantLock();

    private static volatile ClientManagerImpl singleton = null;

    private ClientManagerRegistry() {
    }

    /**
     * Register {@link Client} to the appointed manager by manager id, start the manager if it is created newly.
     *
     * <p>Different client would share the same {@link ClientManager} if they have the same manager id.
     *
     * @param client client to register.
     * @return the client manager which is started.
     */
    public static ClientManager registerClient(Client client) {
        CLIENT_IDS_LOCK.lock();
        try {
            if (null == singleton) {
                final ClientManagerImpl clientManager = new ClientManagerImpl();
                clientManager.startAsync().awaitRunning();
                singleton = clientManager;
            }
            CLIENT_IDS.add(client.getClientId());
            singleton.registerClient(client);
            return singleton;
        } finally {
            CLIENT_IDS_LOCK.unlock();
        }
    }

    /**
     * Unregister {@link Client} to the appointed manager by message id, shutdown the manager if no client
     * registered in it.
     *
     * @param client client to unregister.
     * @return {@link ClientManager} is removed or not.
     */
    @SuppressWarnings("UnusedReturnValue")
    public static boolean unregisterClient(Client client) {
        ClientManagerImpl clientManager = null;
        CLIENT_IDS_LOCK.lock();
        try {
            CLIENT_IDS.remove(client.getClientId());
            singleton.unregisterClient(client);
            if (CLIENT_IDS.isEmpty()) {
                clientManager = singleton;
                singleton = null;
            }
        } finally {
            CLIENT_IDS_LOCK.unlock();
        }
        // No need to hold the lock here.
        if (null != clientManager) {
            clientManager.stopAsync().awaitTerminated();
        }
        return null != clientManager;
    }
}
