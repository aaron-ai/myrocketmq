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

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.PrintThreadStackTraceCommand;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.ThreadStackTrace;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.exception.ResourceNotFoundException;
import org.apache.rocketmq.apis.exception.TelemetryException;
import org.apache.rocketmq.grpcclient.remoting.Signature;
import org.apache.rocketmq.grpcclient.route.AddressScheme;
import org.apache.rocketmq.grpcclient.route.Endpoints;
import org.apache.rocketmq.grpcclient.route.TopicRouteData;
import org.apache.rocketmq.grpcclient.route.TopicRouteDataResult;
import org.apache.rocketmq.grpcclient.utility.ExecutorServices;
import org.apache.rocketmq.grpcclient.utility.MixAll;
import org.apache.rocketmq.grpcclient.utility.ThreadFactoryImpl;
import org.apache.rocketmq.grpcclient.utility.UtilAll;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;

@SuppressWarnings({"UnstableApiUsage", "NullableProblems"})
public abstract class ClientImpl extends AbstractIdleService implements Client {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientImpl.class);

    private static final Duration AWAIT_SETTINGS_APPLIED_DURATION = Duration.ofSeconds(30);

    protected volatile ClientManager clientManager;
    protected final ClientConfiguration clientConfiguration;
    protected final Endpoints accessEndpoints;

    private volatile ScheduledFuture<?> updateRouteCacheFuture;

    protected final Set<String> topics;
    private final ConcurrentMap<String, TopicRouteDataResult> topicRouteResultCache;

    @GuardedBy("inflightRouteFutureLock")
    private final Map<String /* topic */, Set<SettableFuture<TopicRouteDataResult>>> inflightRouteFutureTable;
    private final Lock inflightRouteFutureLock;

    @GuardedBy("telemetryReqObserverTableLock")
    private final ConcurrentMap<Endpoints, TelemetryRespObserver> telemetryResponseObserverTable;
    private final ReadWriteLock telemetryResponseObserverTableLock;

    @GuardedBy("isolatedLock")
    protected final Set<Endpoints> isolated;
    protected final ReadWriteLock isolatedLock;

    /**
     * Telemetry command executor, which is aims to execute commands from remote.
     */
    protected final ThreadPoolExecutor telemetryCommandExecutor;

    protected final String clientId;

    public ClientImpl(ClientConfiguration clientConfiguration, Set<String> topics) {
        this.clientConfiguration = checkNotNull(clientConfiguration, "clientConfiguration should not be null");
        final String accessPoint = clientConfiguration.getAccessPoint();
        boolean httpPatternMatched = accessPoint.startsWith(MixAll.HTTP_PREFIX) || accessPoint.startsWith(MixAll.HTTPS_PREFIX);
        if (httpPatternMatched) {
            final int startIndex = accessPoint.lastIndexOf('/') + 1;
            final String[] split = accessPoint.substring(startIndex).split(":");
            String domainName = split[0];
            domainName = domainName.replace("_", "-").toLowerCase(UtilAll.LOCALE);
            int port = split.length >= 2 ? Integer.parseInt(split[1]) : 80;
            this.accessEndpoints = new Endpoints(AddressScheme.DOMAIN_NAME.getPrefix() + domainName + ":" + port);
        } else {
            this.accessEndpoints = new Endpoints(accessPoint);
        }
        this.topics = topics;
        // Generate client id firstly.
        this.clientId = UtilAll.genClientId();

        this.topicRouteResultCache = new ConcurrentHashMap<>();

        this.inflightRouteFutureTable = new ConcurrentHashMap<>();
        this.inflightRouteFutureLock = new ReentrantLock();

        this.telemetryResponseObserverTable = new ConcurrentHashMap<>();
        this.telemetryResponseObserverTableLock = new ReentrantReadWriteLock();

        this.isolated = new HashSet<>();
        this.isolatedLock = new ReentrantReadWriteLock();

        this.telemetryCommandExecutor = new ThreadPoolExecutor(
            1,
            1,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("CommandExecutor"));
    }

    /**
     * Start the rocketmq client and do some preparatory work.
     */
    @Override
    protected void startUp() throws Exception {
        LOGGER.info("Begin to start the rocketmq client, clientId={}", clientId);
        // Register client after client id generation.
        this.clientManager = ClientManagerRegistry.registerClient(this);
        final ScheduledExecutorService scheduler = clientManager.getScheduler();
        // Fetch topic route from remote.
        LOGGER.info("Begin to fetch topic(s) route data from remote during client startup, clientId={}, topics={}", clientId, topics);
        // Aggregate all topic route data futures into a composited future.
        final List<ListenableFuture<TopicRouteDataResult>> futures = topics.stream()
            .map(this::getRouteDataResult)
            .collect(Collectors.toList());
        List<TopicRouteDataResult> results;
        try {
            results = Futures.allAsList(futures).get();
        } catch (Throwable t) {
            LOGGER.error("Failed to get topic route data result from remote during client startup, clientId={}, topics={}", clientId, topics, t);
            throw new RuntimeException(t);
        }
        // Find any topic whose topic route data is failed to fetch from remote.
        final Stream<TopicRouteDataResult> stream = results.stream()
            .filter(topicRouteDataResult -> Code.OK != topicRouteDataResult.getStatus().getCode());
        final Optional<TopicRouteDataResult> any = stream.findAny();
        // There is a topic whose topic route data is failed to fetch from remote.
        if (any.isPresent()) {
            final TopicRouteDataResult result = any.get();
            final Status status = result.getStatus();
            // TODO: polish code here.
            throw new ResourceNotFoundException(status.getCode().ordinal(), status.getMessage());
        }
        LOGGER.info("Fetch topic route data from remote successfully during startup, clientId={}, topics={}", clientId, topics);
        // Report active settings during startup.
        this.announceSettings();
        this.awaitFirstSettingApplied(AWAIT_SETTINGS_APPLIED_DURATION);
        // Update route cache periodically.
        this.updateRouteCacheFuture = scheduler.scheduleWithFixedDelay(() -> {
            try {
                updateRouteCache();
            } catch (Throwable t) {
                LOGGER.error("Exception raised while updating topic route cache, clientId={}", clientId, t);
            }
        }, 10, 30, TimeUnit.SECONDS);
        LOGGER.info("The rocketmq client starts successfully, clientId={}", clientId);
    }

    /**
     * Shutdown the rocketmq client and release related resources.
     */
    @Override
    protected void shutDown() throws InterruptedException {
        LOGGER.info("Begin to shutdown the rocketmq client, clientId={}", clientId);
        notifyClientTermination();
        if (null != this.updateRouteCacheFuture) {
            updateRouteCacheFuture.cancel(false);
        }
        telemetryCommandExecutor.shutdown();
        if (!ExecutorServices.awaitTerminated(telemetryCommandExecutor)) {
            LOGGER.error("[Bug] Timeout to shutdown the telemetry command executor");
        } else {
            LOGGER.info("Shutdown the telemetry command executor successfully");
        }
        ClientManagerRegistry.unregisterClient(this);
        LOGGER.info("Shutdown the rocketmq client successfully, clientId={}", clientId);
    }

    @SuppressWarnings("SameParameterValue")
    abstract protected void awaitFirstSettingApplied(
        Duration duration) throws ExecutionException, InterruptedException, TimeoutException;

    /**
     * @see Client#onPrintThreadStackCommand(Endpoints, PrintThreadStackTraceCommand)
     */
    @Override
    public void onPrintThreadStackCommand(Endpoints endpoints, PrintThreadStackTraceCommand command) {
        final String nonce = command.getNonce();
        Runnable task = () -> {
            try {
                final String stackTrace = UtilAll.stackTrace();
                Status status = Status.newBuilder().setCode(Code.OK).build();
                ThreadStackTrace threadStackTrace = ThreadStackTrace.newBuilder().setThreadStackTrace(stackTrace)
                    .setNonce(command.getNonce()).setStatus(status).build();
                TelemetryCommand telemetryCommand = TelemetryCommand.newBuilder().setThreadStackTrace(threadStackTrace).build();
                telemetryCommand(endpoints, telemetryCommand);
            } catch (Throwable t) {
                LOGGER.error("Failed to send thread stack trace to remote, endpoints={}, nonce={}, clientId={}", endpoints, nonce, clientId, t);
            }
        };
        try {
            telemetryCommandExecutor.submit(task);
        } catch (Throwable t) {
            LOGGER.error("[Bug] Exception raised while submitting task to print thread stack trace, endpoints={}, nonce={}, clientId={}", endpoints, nonce, clientId, t);
        }
    }

    /**
     * Get client local settings and convert it to protobuf message.
     */
    public abstract Settings localSettings();

    /**
     * @see Client#announceSettings()
     */
    @Override
    public void announceSettings() throws Exception {
        final Settings settings = localSettings();
        final TelemetryCommand command = TelemetryCommand.newBuilder().setSettings(settings).build();
        final Set<Endpoints> totalRouteEndpoints = getTotalRouteEndpoints();
        Exception exception = null;
        for (Endpoints endpoints : totalRouteEndpoints) {
            try {
                telemetryCommand(endpoints, command);
            } catch (Exception e) {
                LOGGER.error("Failed to announce settings to remote, clientId={}, endpoints={}", clientId, endpoints, e);
                exception = e;
            }
        }
        if (null != exception) {
            throw exception;
        }
    }

    public void telemetryCommand(Endpoints endpoints, TelemetryCommand command) throws TelemetryException {
        StreamObserver<TelemetryCommand> requestObserver;
        try {
            final TelemetryRespObserver responseObserver = this.getTelemetryRespObserver(endpoints);
            Metadata metadata = sign();
            requestObserver = clientManager.telemetry(endpoints, metadata, Duration.ofNanos(Long.MAX_VALUE), responseObserver);
        } catch (Throwable t) {
            throw new TelemetryException("Failed to send telemetry command", t);
        }
        try {
            requestObserver.onNext(command);
        } catch (RuntimeException e) {
            requestObserver.onError(e);
            throw e;
        }
        requestObserver.onCompleted();
    }

    /**
     * Get gRPC response observer by endpoint.
     */
    public TelemetryRespObserver getTelemetryRespObserver(Endpoints endpoints) {
        telemetryResponseObserverTableLock.readLock().lock();
        TelemetryRespObserver responseObserver;
        try {
            responseObserver = telemetryResponseObserverTable.get(endpoints);
            if (null != responseObserver) {
                return responseObserver;
            }
        } finally {
            telemetryResponseObserverTableLock.readLock().unlock();
        }
        telemetryResponseObserverTableLock.writeLock().lock();
        try {
            responseObserver = telemetryResponseObserverTable.get(endpoints);
            if (null != responseObserver) {
                return responseObserver;
            }
            responseObserver = new TelemetryRespObserver(this, endpoints);
            telemetryResponseObserverTable.put(endpoints, responseObserver);
            return responseObserver;
        } finally {
            telemetryResponseObserverTableLock.writeLock().unlock();
        }
    }

    /**
     * Triggered when {@link TopicRouteDataResult} is fetched from remote.
     */
    public final void onTopicRouteDataResultUpdate(String topic, TopicRouteDataResult topicRouteDataResult) {
        final TopicRouteDataResult old = topicRouteResultCache.put(topic, topicRouteDataResult);
        // TODO: still update topic route if the result is not OK.
        if (topicRouteDataResult.equals(old)) {
            LOGGER.info("Topic route remains the same, topic={}, clientId={}", topic, clientId);
        } else {
            LOGGER.info("Topic route is updated, topic={}, clientId={}, {} => {}", old, topicRouteDataResult);
        }
        onTopicRouteDataResultUpdate0(topic, topicRouteDataResult);
    }

    public void onTopicRouteDataResultUpdate0(String topic, TopicRouteDataResult topicRouteDataResult) {
    }

    private void updateRouteCache() {
        LOGGER.info("Start to update route cache for a new round, clientId={}", clientId);
        topicRouteResultCache.keySet().forEach(topic -> {
            final ListenableFuture<TopicRouteDataResult> future = fetchTopicRoute(topic);
            Futures.addCallback(future, new FutureCallback<TopicRouteDataResult>() {
                @Override
                public void onSuccess(TopicRouteDataResult topicRouteDataResult) {
                    final TopicRouteDataResult old = topicRouteResultCache.put(topic, topicRouteDataResult);
                    // TODO: still update topic route if the result is not OK.
                    if (topicRouteDataResult.equals(old)) {
                        LOGGER.info("Topic route remains the same, topic={}, clientId={}", topic, clientId);
                    } else {
                        LOGGER.info("Topic route is updated, topic={}, clientId={}, {} => {}", old, topicRouteDataResult);
                    }
                    onTopicRouteDataResultUpdate(topic, topicRouteDataResult);
                }

                @Override
                public void onFailure(Throwable t) {
                    LOGGER.error("Failed to fetch topic route for update cache, topic={}, clientId={}", topic, clientId, t);
                }
            }, MoreExecutors.directExecutor());
        });
    }

    /**
     * Wrap notify client termination request.
     */
    public abstract NotifyClientTerminationRequest wrapNotifyClientTerminationRequest();

    /**
     * Notify remote that current client is prepared to be terminated.
     */
    private void notifyClientTermination() {
        LOGGER.info("Notify that client is terminated, clientId={}", clientId);
        final Set<Endpoints> routeEndpointsSet = getTotalRouteEndpoints();
        final NotifyClientTerminationRequest notifyClientTerminationRequest = wrapNotifyClientTerminationRequest();
        try {
            final Metadata metadata = sign();
            for (Endpoints endpoints : routeEndpointsSet) {
                clientManager.notifyClientTermination(endpoints, metadata, notifyClientTerminationRequest,
                    clientConfiguration.getRequestTimeout());
            }
        } catch (Throwable t) {
            LOGGER.error("Exception raised while notifying client's termination, clientId={}", clientId, t);
        }
    }

    /**
     * @see Client#getClientId()
     */
    @Override
    public String getClientId() {
        return clientId;
    }

    /**
     * @see Client#doHeartbeat()
     */
    @Override
    public void doHeartbeat() {
        final Set<Endpoints> totalEndpoints = getTotalRouteEndpoints();
        final HeartbeatRequest request = wrapHeartbeatRequest();
        for (Endpoints endpoints : totalEndpoints) {
            doHeartbeat(request, endpoints);
        }
    }

    /**
     * Real-time signature generation
     */
    public Metadata sign() throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeyException {
        return Signature.sign(clientConfiguration, clientId);
    }

    /**
     * Send heartbeat data to appointed endpoint
     *
     * @param request   heartbeat data request
     * @param endpoints endpoint to send heartbeat data
     */
    private void doHeartbeat(HeartbeatRequest request, final Endpoints endpoints) {
        try {
            Metadata metadata = sign();
            final ListenableFuture<HeartbeatResponse> future = clientManager
                .heartbeat(endpoints, metadata, request, clientConfiguration.getRequestTimeout());
            Futures.addCallback(future, new FutureCallback<HeartbeatResponse>() {
                @Override
                public void onSuccess(HeartbeatResponse response) {
                    final Status status = response.getStatus();
                    final Code code = status.getCode();
                    if (Code.OK != code) {
                        LOGGER.warn("Failed to send heartbeat, code={}, status message=[{}], endpoints={}, clientId={}",
                            code, status.getMessage(), endpoints, clientId);
                        return;
                    }
                    LOGGER.info("Send heartbeat successfully, endpoints={}, clientId={}", endpoints, clientId);
                    isolatedLock.writeLock().lock();
                    try {
                        final boolean removed = isolated.remove(endpoints);
                        if (removed) {
                            LOGGER.info("Rejoin endpoints which is isolated before, clientId={}, endpoints={}", clientId, endpoints);
                        }
                    } finally {
                        isolatedLock.writeLock().unlock();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    LOGGER.warn("Failed to send heartbeat, endpoints={}, clientId={}", endpoints, clientId, t);
                }
            }, MoreExecutors.directExecutor());
        } catch (Throwable e) {
            LOGGER.error("Exception raised while preparing heartbeat, endpoints={}, clientId={}", endpoints, clientId, e);
        }
    }

    /**
     * Wrap heartbeat request
     */
    public abstract HeartbeatRequest wrapHeartbeatRequest();

    /**
     * @see Client#doStats()
     */
    @Override
    public void doStats() {
    }

    private ListenableFuture<TopicRouteDataResult> fetchTopicRoute(final String topic) {
        final SettableFuture<TopicRouteDataResult> future = SettableFuture.create();
        try {
            Resource topicResource = Resource.newBuilder().setName(topic).build();
            final QueryRouteRequest request = QueryRouteRequest.newBuilder().setTopic(topicResource)
                .setEndpoints(accessEndpoints.toProtobuf()).build();
            final Metadata metadata = sign();
            final ListenableFuture<QueryRouteResponse> responseFuture =
                clientManager.queryRoute(accessEndpoints, metadata, request, clientConfiguration.getRequestTimeout());
            return Futures.transform(responseFuture, response -> {
                final Status status = response.getStatus();
                final Code code = status.getCode();
                if (Code.OK != code) {
                    LOGGER.error("Exception raised while fetch topic route from remote, topic={}, " +
                            "clientId={}, accessPoint={}, code={}, status message=[{}]", topic, clientId,
                        accessEndpoints, code, status.getMessage());
                }
                return new TopicRouteDataResult(new TopicRouteData(response.getMessageQueuesList()), status);
            }, MoreExecutors.directExecutor());
        } catch (Throwable t) {
            future.setException(t);
            return future;
        }
    }

    protected Set<Endpoints> getTotalRouteEndpoints() {
        Set<Endpoints> totalRouteEndpoints = new HashSet<>();
        for (TopicRouteDataResult result : topicRouteResultCache.values()) {
            totalRouteEndpoints.addAll(result.getTopicRouteData().getTotalEndpoints());
        }
        return totalRouteEndpoints;
    }

    protected ListenableFuture<TopicRouteDataResult> getRouteDataResult(final String topic) {
        SettableFuture<TopicRouteDataResult> future0 = SettableFuture.create();
        TopicRouteDataResult topicRouteDataResult = topicRouteResultCache.get(topic);
        // If route result was cached before, get it directly.
        if (null != topicRouteDataResult) {
            future0.set(topicRouteDataResult);
            return future0;
        }
        inflightRouteFutureLock.lock();
        try {
            // If route was fetched by last in-flight request, get it directly.
            topicRouteDataResult = topicRouteResultCache.get(topic);
            if (null != topicRouteDataResult) {
                future0.set(topicRouteDataResult);
                return future0;
            }
            Set<SettableFuture<TopicRouteDataResult>> inflightFutures = inflightRouteFutureTable.get(topic);
            // Request is in-flight, return future directly.
            if (null != inflightFutures) {
                inflightFutures.add(future0);
                return future0;
            }
            inflightFutures = new HashSet<>();
            inflightFutures.add(future0);
            inflightRouteFutureTable.put(topic, inflightFutures);
        } finally {
            inflightRouteFutureLock.unlock();
        }
        final ListenableFuture<TopicRouteDataResult> future = fetchTopicRoute(topic);
        Futures.addCallback(future, new FutureCallback<TopicRouteDataResult>() {
            @Override
            public void onSuccess(TopicRouteDataResult result) {
                inflightRouteFutureLock.lock();
                try {
                    onTopicRouteDataResultUpdate(topic, result);
                    final Set<SettableFuture<TopicRouteDataResult>> newFutureSet =
                        inflightRouteFutureTable.remove(topic);
                    if (null == newFutureSet) {
                        // Should never reach here.
                        LOGGER.error("[Bug] in-flight route futures was empty, topic={}, clientId={}", topic, clientId);
                        return;
                    }
                    LOGGER.debug("Fetch topic route successfully, topic={}, in-flight route future "
                        + "size={}, clientId={}", topic, newFutureSet.size(), clientId);
                    for (SettableFuture<TopicRouteDataResult> newFuture : newFutureSet) {
                        newFuture.set(result);
                    }
                } catch (Throwable t) {
                    // Should never reach here.
                    LOGGER.error("[Bug] Exception raised while update route data, topic={}, clientId={}", topic, clientId, t);
                } finally {
                    inflightRouteFutureLock.unlock();
                }
            }

            @Override
            public void onFailure(Throwable t) {
                inflightRouteFutureLock.lock();
                try {
                    final Set<SettableFuture<TopicRouteDataResult>> newFutureSet =
                        inflightRouteFutureTable.remove(topic);
                    if (null == newFutureSet) {
                        // Should never reach here.
                        LOGGER.error("[Bug] in-flight route futures was empty, topic={}, clientId={}", topic, clientId);
                        return;
                    }
                    LOGGER.error("Failed to fetch topic route, topic={}, in-flight route future " +
                        "size={}, clientId={}", topic, newFutureSet.size(), clientId, t);
                    for (SettableFuture<TopicRouteDataResult> future : newFutureSet) {
                        future.setException(t);
                    }
                } finally {
                    inflightRouteFutureLock.unlock();
                }
            }
        }, MoreExecutors.directExecutor());
        return future0;
    }

    public ScheduledExecutorService getScheduler() {
        return clientManager.getScheduler();
    }
}
