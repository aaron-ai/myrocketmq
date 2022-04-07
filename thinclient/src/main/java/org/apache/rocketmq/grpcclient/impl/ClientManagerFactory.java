package org.apache.rocketmq.grpcclient.impl;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@ThreadSafe
public class ClientManagerFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientManagerFactory.class);

    private static final ClientManagerFactory INSTANCE = new ClientManagerFactory();

    @GuardedBy("managersTableLock")
    private final Map<String, ClientManagerImpl> managersTable;
    private final Lock managersTableLock;

    private ClientManagerFactory() {
        this.managersTable = new HashMap<>();
        this.managersTableLock = new ReentrantLock();
    }

    public static ClientManagerFactory getInstance() {
        return INSTANCE;
    }

    /**
     * Register {@link Client} to the appointed manager by manager id, start the manager if it is created newly.
     *
     * <p>Different client would share the same {@link ClientManager} if they have the same manager id.
     *
     * @param managerId client manager id.
     * @param client    client to register.
     * @return the client manager which is started.
     */
    public ClientManager registerClient(String managerId, Client client) {
        managersTableLock.lock();
        try {
            ClientManagerImpl manager = managersTable.get(managerId);
            if (null == manager) {
                // create and start manager.
                manager = new ClientManagerImpl(managerId);
                managersTable.put(managerId, manager);
            }
            manager.registerClient(client);
            return manager;
        } finally {
            managersTableLock.unlock();
        }
    }

    /**
     * Unregister {@link Client} to the appointed manager by message id, shutdown the manager if no client
     * registered in it.
     *
     * @param managerId identification of client manager.
     * @param client    client to unregister.
     * @return {@link ClientManager} is removed or not.
     */
    @SuppressWarnings("UnusedReturnValue")
    public boolean unregisterClient(String managerId, Client client) throws IOException {
        ClientManagerImpl removedManager = null;
        managersTableLock.lock();
        try {
            final ClientManagerImpl manager = managersTable.get(managerId);
            if (null == manager) {
                // should never reach here.
                LOGGER.error("[Bug] manager not found by managerId={}", managerId);
                return false;
            }
            manager.unregisterClient(client);
            // shutdown the manager if no client registered.
            if (manager.isEmpty()) {
                removedManager = manager;
                managersTable.remove(managerId);
            }
        } finally {
            managersTableLock.unlock();
        }
        // no need to hold the lock here.
        if (null != removedManager) {
            removedManager.close();
        }
        return null != removedManager;
    }
}
