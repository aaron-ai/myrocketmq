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

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.EndTransactionResponse;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v1.HealthCheckRequest;
import apache.rocketmq.v1.HealthCheckResponse;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.HeartbeatResponse;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.NotifyClientTerminationRequest;
import apache.rocketmq.v1.NotifyClientTerminationResponse;
import apache.rocketmq.v1.PollCommandRequest;
import apache.rocketmq.v1.PollCommandResponse;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.PullMessageResponse;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryAssignmentResponse;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.QueryOffsetResponse;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import apache.rocketmq.v1.ReportMessageConsumptionResultRequest;
import apache.rocketmq.v1.ReportMessageConsumptionResultResponse;
import apache.rocketmq.v1.ReportThreadStackTraceRequest;
import apache.rocketmq.v1.ReportThreadStackTraceResponse;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import io.grpc.Metadata;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.exception.ErrorCode;
import org.apache.rocketmq.apis.exception.SslException;
import org.apache.rocketmq.grpcclient.remoting.RpcClient;
import org.apache.rocketmq.grpcclient.remoting.RpcClientImpl;
import org.apache.rocketmq.grpcclient.route.Endpoints;
import org.apache.rocketmq.grpcclient.utility.ExecutorServices;
import org.apache.rocketmq.grpcclient.utility.MetadataUtils;
import org.apache.rocketmq.grpcclient.utility.ThreadFactoryImpl;
import org.slf4j.bridge.SLF4JBridgeHandler;

import javax.net.ssl.SSLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@SuppressWarnings("UnstableApiUsage")
public class ClientManagerImpl extends AbstractIdleService implements ClientManager {
    public static final long RPC_CLIENT_MAX_IDLE_SECONDS = 30 * 60;
    public static final long HEALTH_CHECK_PERIOD_SECONDS = 15;
    public static final long IDLE_RPC_CLIENT_PERIOD_SECONDS = 60;
    public static final long HEART_BEAT_PERIOD_SECONDS = 10;
    public static final long LOG_STATS_PERIOD_SECONDS = 60;

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientManagerImpl.class);
    /**
     * Client manager id.
     */
    private final String id;

    @GuardedBy("rpcClientTableLock")
    private final Map<Endpoints, RpcClient> rpcClientTable;
    private final ReadWriteLock rpcClientTableLock;

    /**
     * Contains all client, key is {@link ClientImpl#clientId}.
     */
    private final ConcurrentMap<String, Client> clientTable;

    /**
     * In charge of all scheduled task.
     */
    private final ScheduledExecutorService scheduler;

    /**
     * Public executor for all async rpc, <strong>should never submit heavy task.</strong>
     */
    private final ExecutorService asyncWorker;

    public ClientManagerImpl(String id) {
        this.id = id;

        this.rpcClientTable = new HashMap<>();
        this.rpcClientTableLock = new ReentrantReadWriteLock();

        this.clientTable = new ConcurrentHashMap<>();

        this.scheduler = new ScheduledThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryImpl("ClientScheduler"));

        this.asyncWorker = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryImpl("ClientAsyncWorker"));
    }

    static {
        // redirect JUL logging to slf4j.
        // see https://github.com/grpc/grpc-java/issues/1577
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    @Override
    public void registerClient(Client client) {
        clientTable.put(client.getClientId(), client);
    }

    @Override
    public void unregisterClient(Client client) {
        clientTable.remove(client.getClientId());
    }

    @Override
    public boolean isEmpty() {
        return clientTable.isEmpty();
    }

    @Override
    protected void startUp() {
        LOGGER.info("Begin to start the client manager, clientManagerId={}", id);
        scheduler.scheduleWithFixedDelay(
                () -> {
                    try {
                        doHealthCheck();
                    } catch (Throwable t) {
                        LOGGER.error("Exception raised while health check.", t);
                    }
                },
                5,
                HEALTH_CHECK_PERIOD_SECONDS,
                TimeUnit.SECONDS
        );

        scheduler.scheduleWithFixedDelay(
                () -> {
                    try {
                        clearIdleRpcClients();
                    } catch (Throwable t) {
                        LOGGER.error("Exception raised while clear idle rpc clients.", t);
                    }
                },
                5,
                IDLE_RPC_CLIENT_PERIOD_SECONDS,
                TimeUnit.SECONDS);

        scheduler.scheduleWithFixedDelay(
                () -> {
                    try {
                        doHeartbeat();
                    } catch (Throwable t) {
                        LOGGER.error("Exception raised while heartbeat.", t);
                    }
                },
                1,
                HEART_BEAT_PERIOD_SECONDS,
                TimeUnit.SECONDS
        );

        scheduler.scheduleWithFixedDelay(
                () -> {
                    try {
                        doLogStats();
                    } catch (Throwable t) {
                        LOGGER.error("Exception raised while log stats.", t);
                    }
                },
                1,
                LOG_STATS_PERIOD_SECONDS,
                TimeUnit.SECONDS
        );
        LOGGER.info("The client manager starts successfully, clientManagerId={}", id);
    }

    @Override
    protected void shutDown() throws Exception {
        LOGGER.info("Begin to shutdown the client manager, clientManagerId={}", id);
        scheduler.shutdown();
        if (!ExecutorServices.awaitTerminated(scheduler)) {
            LOGGER.error("[Bug] Timeout to shutdown the client scheduler, clientManagerId={}", id);
        } else {
            LOGGER.info("Shutdown the client scheduler successfully, clientManagerId={}", id);
        }
        rpcClientTableLock.writeLock().lock();
        try {
            final Iterator<Map.Entry<Endpoints, RpcClient>> it = rpcClientTable.entrySet().iterator();
            while (it.hasNext()) {
                final Map.Entry<Endpoints, RpcClient> entry = it.next();
                final RpcClient rpcClient = entry.getValue();
                it.remove();
                rpcClient.shutdown();
            }
        } finally {
            rpcClientTableLock.writeLock().unlock();
        }
        LOGGER.info("Shutdown all rpc client(s) successfully, clientManagerId={}", id);
        asyncWorker.shutdown();
        if (!ExecutorServices.awaitTerminated(asyncWorker)) {
            LOGGER.error("[Bug] Timeout to shutdown the client async worker, clientManagerId={}", id);
        } else {
            LOGGER.info("Shutdown the client async worker successfully, clientManagerId={}", id);
        }
        LOGGER.info("Shutdown the client manager successfully, clientManagerId={}", id);
    }

    private void doHealthCheck() {
        LOGGER.info("Start to do health check for a new round, clientManagerId={}", id);
        for (Client client : clientTable.values()) {
            client.doHealthCheck();
        }
    }

    /**
     * It is well-founded that a {@link RpcClient} is deprecated if it is idle for a long time, so it is essential to
     * clear it.
     *
     * @throws InterruptedException if thread has been interrupted
     */
    private void clearIdleRpcClients() throws InterruptedException {
        rpcClientTableLock.writeLock().lock();
        try {
            final Iterator<Map.Entry<Endpoints, RpcClient>> it = rpcClientTable.entrySet().iterator();
            while (it.hasNext()) {
                final Map.Entry<Endpoints, RpcClient> entry = it.next();
                final Endpoints endpoints = entry.getKey();
                final RpcClient client = entry.getValue();

                final long idleSeconds = client.idleSeconds();
                if (idleSeconds > RPC_CLIENT_MAX_IDLE_SECONDS) {
                    it.remove();
                    client.shutdown();
                    LOGGER.info("Rpc client has been idle for a long time, endpoints={}, idleSeconds={}, "
                            + "maxIdleSeconds={}", endpoints, idleSeconds, RPC_CLIENT_MAX_IDLE_SECONDS);
                }
            }
        } finally {
            rpcClientTableLock.writeLock().unlock();
        }
    }

    private void doHeartbeat() {
        for (Client client : clientTable.values()) {
            client.doHeartbeat();
        }
    }

    private void doLogStats() {
        LOGGER.info("Start to log stats for a new round, clientVersion={}, clientWrapperVersion={}, clientManagerId={}",
                MetadataUtils.getVersion(), MetadataUtils.getWrapperVersion(), id);
        for (Client client : clientTable.values()) {
            client.doStats();
        }
    }

    /**
     * Return rpc client by remote {@link Endpoints}, would create client automatically if it does not exist.
     *
     * <p>In case of the occasion that {@link RpcClient} is garbage collected before shutdown when invoked
     * concurrently, lock here is essential.
     *
     * @param endpoints remote endpoints.
     * @return rpc client.
     */
    private RpcClient getRpcClient(Endpoints endpoints) throws ClientException {
        RpcClient rpcClient;
        rpcClientTableLock.readLock().lock();
        try {
            rpcClient = rpcClientTable.get(endpoints);
            if (null != rpcClient) {
                return rpcClient;
            }
        } finally {
            rpcClientTableLock.readLock().unlock();
        }
        rpcClientTableLock.writeLock().lock();
        try {
            rpcClient = rpcClientTable.get(endpoints);
            if (null != rpcClient) {
                return rpcClient;
            }
            try {
                rpcClient = new RpcClientImpl(endpoints);
            } catch (SSLException e) {
                LOGGER.error("Failed to get rpc client, endpoints={}", endpoints);
                throw new SslException(ErrorCode.SSL_CONTEXT_INITIALIZATION_FAILURE, "Failed to generate RPC client",
                        e);
            }
            rpcClientTable.put(endpoints, rpcClient);
            return rpcClient;
        } finally {
            rpcClientTableLock.writeLock().unlock();
        }
    }

    @Override
    public ListenableFuture<QueryRouteResponse> queryRoute(Endpoints endpoints, Metadata metadata,
                                                           QueryRouteRequest request, long duration,
                                                           TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.queryRoute(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<QueryRouteResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<HeartbeatResponse> heartbeat(Endpoints endpoints, Metadata metadata,
                                                         HeartbeatRequest request, long duration, TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.heartbeat(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<HeartbeatResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<HealthCheckResponse> healthCheck(Endpoints endpoints, Metadata metadata,
                                                             HealthCheckRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.healthCheck(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<HealthCheckResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<SendMessageResponse> sendMessage(Endpoints endpoints, Metadata metadata,
                                                             SendMessageRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.sendMessage(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<SendMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<QueryAssignmentResponse> queryAssignment(Endpoints endpoints, Metadata metadata,
                                                                     QueryAssignmentRequest request, long duration,
                                                                     TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.queryAssignment(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<QueryAssignmentResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<ReceiveMessageResponse> receiveMessage(Endpoints endpoints, Metadata metadata,
                                                                   ReceiveMessageRequest request, long duration,
                                                                   TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.receiveMessage(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<ReceiveMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<AckMessageResponse> ackMessage(Endpoints endpoints, Metadata metadata,
                                                           AckMessageRequest request, long duration,
                                                           TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.ackMessage(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<AckMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<NackMessageResponse> nackMessage(Endpoints endpoints, Metadata metadata,
                                                             NackMessageRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.nackMessage(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<NackMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(
            Endpoints endpoints, Metadata metadata, ForwardMessageToDeadLetterQueueRequest request, long duration,
            TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.forwardMessageToDeadLetterQueue(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<ForwardMessageToDeadLetterQueueResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<EndTransactionResponse> endTransaction(Endpoints endpoints, Metadata metadata,
                                                                   EndTransactionRequest request, long duration,
                                                                   TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.endTransaction(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            SettableFuture<EndTransactionResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<QueryOffsetResponse> queryOffset(Endpoints endpoints, Metadata metadata,
                                                             QueryOffsetRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.queryOffset(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<QueryOffsetResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<PullMessageResponse> pullMessage(Endpoints endpoints, Metadata metadata,
                                                             PullMessageRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.pullMessage(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<PullMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<PollCommandResponse> pollCommand(Endpoints endpoints, Metadata metadata,
                                                             PollCommandRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.pollCommand(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<PollCommandResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<ReportThreadStackTraceResponse> reportThreadStackTrace(
            Endpoints endpoints, Metadata metadata, ReportThreadStackTraceRequest request, long duration,
            TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.reportThreadStackTrace(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<ReportThreadStackTraceResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<ReportMessageConsumptionResultResponse> reportMessageConsumption(
            Endpoints endpoints, Metadata metadata, ReportMessageConsumptionResultRequest request, long duration,
            TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.reportMessageConsumptionResult(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<ReportMessageConsumptionResultResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<NotifyClientTerminationResponse> notifyClientTermination(
            Endpoints endpoints, Metadata metadata, NotifyClientTerminationRequest request, long duration,
            TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.notifyClientTermination(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<NotifyClientTerminationResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ScheduledExecutorService getScheduler() {
        return this.scheduler;
    }
}
