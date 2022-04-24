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

import apache.rocketmq.v2.ApplyPassiveSettingsCommand;
import apache.rocketmq.v2.ApplyPassiveSettingsResult;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.ReportActiveSettingsCommand;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Status;
import com.google.common.collect.Sets;
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
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.exception.ResourceNotFoundException;
import org.apache.rocketmq.grpcclient.remoting.Signature;
import org.apache.rocketmq.grpcclient.route.AddressScheme;
import org.apache.rocketmq.grpcclient.route.Endpoints;
import org.apache.rocketmq.grpcclient.route.TopicRouteData;
import org.apache.rocketmq.grpcclient.route.TopicRouteDataResult;
import org.apache.rocketmq.grpcclient.utility.MixAll;
import org.apache.rocketmq.grpcclient.utility.UtilAll;

import java.io.IOException;
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

    private final AtomicLong nonceIndex = new AtomicLong(0);

    protected final String namespace;
    protected volatile ClientManager clientManager;
    protected final ClientConfiguration clientConfiguration;
    private final Endpoints accessEndpoints;

    private volatile ScheduledFuture<?> updateRouteCacheFuture;

    protected final Set<String> topics;
    private final ConcurrentMap<String, TopicRouteDataResult> topicRouteResultCache;

    @GuardedBy("inflightRouteFutureLock")
    private final Map<String /* topic */, Set<SettableFuture<TopicRouteDataResult>>> inflightRouteFutureTable;
    private final Lock inflightRouteFutureLock;

    @GuardedBy("telemetryReqObserverTableLock")
    private final ConcurrentMap<Endpoints, TelemetryRequestObserver> telemetryReqObserverTable;
    private final ReadWriteLock telemetryReqObserverTableLock;

    final SettableFuture<ApplyPassiveSettingsCommand> startupPassiveSettingsFuture;

    protected final String clientId;

    public ClientImpl(ClientConfiguration clientConfiguration, Set<String> topics) {
        this.clientConfiguration = checkNotNull(clientConfiguration, "clientConfiguration should not be null");
        final String accessPoint = clientConfiguration.getAccessPoint();
        boolean httpPatternMatched = accessPoint.startsWith(MixAll.HTTP_PREFIX) || accessPoint.startsWith(MixAll.HTTPS_PREFIX);
        if (httpPatternMatched) {
            final int startIndex = accessPoint.lastIndexOf('/') + 1;
            this.namespace = accessPoint.substring(startIndex, accessPoint.indexOf('.'));
            final String[] split = accessPoint.substring(startIndex).split(":");
            String domainName = split[0];
            domainName = domainName.replace("_", "-").toLowerCase(UtilAll.LOCALE);
            int port = split.length >= 2 ? Integer.parseInt(split[1]) : 80;
            this.accessEndpoints = new Endpoints(AddressScheme.DOMAIN_NAME.getPrefix() + domainName + ":" + port);
        } else {
            this.namespace = StringUtils.EMPTY;
            this.accessEndpoints = new Endpoints(accessPoint);
        }
        this.topics = topics;
        // Generate client id firstly.
        this.clientId = UtilAll.genClientId();

        this.topicRouteResultCache = new ConcurrentHashMap<>();

        this.inflightRouteFutureTable = new ConcurrentHashMap<>();
        this.inflightRouteFutureLock = new ReentrantLock();

        this.telemetryReqObserverTable = new ConcurrentHashMap<>();
        this.telemetryReqObserverTableLock = new ReentrantReadWriteLock();

        this.startupPassiveSettingsFuture = SettableFuture.create();
    }

    @Override
    protected void startUp() throws ExecutionException, InterruptedException {
        LOGGER.info("Begin to start the rocketmq client, clientId={}", clientId);
        // Register client after client id generation.
        this.clientManager = ClientManagerFactory.getInstance().registerClient(namespace, this);
        final ScheduledExecutorService scheduler = clientManager.getScheduler();
        // Fetch topic route from remote.
        try {
            LOGGER.info("Begin to fetch topic route data from remote during startup, clientId={}", clientId);
            // Aggregate all topic route data futures into a composited future.
            final List<ListenableFuture<TopicRouteDataResult>> futures = topics.stream()
                .map(this::getRouteDataResult)
                .collect(Collectors.toList());
            final List<TopicRouteDataResult> results = Futures.allAsList(futures).get();
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
            LOGGER.info("Fetch topic route data from remote successfully during startup, clientId={}", clientId);
        } catch (Throwable t) {
            // Should never reach here.
            LOGGER.error("[Bug] Unexpected exception thrown while fetching topic route data from remote during startup, clientId={}", clientId, t);
            throw new RuntimeException(t);
        }
        // Report active settings during startup.
        this.reportActiveSettings().get();
        this.startupPassiveSettingsFuture.get();
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

    protected abstract ReportActiveSettingsCommand wrapReportActiveSettingsCommand();

//    private ApplyPassiveSettingsResult wrapApplyPassiveSettingsResult(String nonce) {
//        final ApplyPassiveSettingsResult result = ApplyPassiveSettingsResult.newBuilder().setNonce(nonce).build();
//    }

    protected abstract void handlePassiveSettingsCommand(ApplyPassiveSettingsCommand command);

    protected String generateClientNonce() {
        return clientId + "_" + nonceIndex.incrementAndGet();
    }

    private ListenableFuture<List<Void>> reportActiveSettings() {
        final Set<Endpoints> totalRouteEndpoints = getTotalRouteEndpoints();
        List<SettableFuture<Void>> futures = new ArrayList<>();
        for (Endpoints endpoints : totalRouteEndpoints) {
            final SettableFuture<Void> future = SettableFuture.create();
            reportActiveSettings0(endpoints, future);
            futures.add(future);
        }
        return Futures.allAsList(futures);
    }

    public TelemetryRequestObserver getTelemetryRequestObserver(Endpoints endpoints) {
        telemetryReqObserverTableLock.readLock().lock();
        TelemetryRequestObserver requestObserver;
        try {
            requestObserver = telemetryReqObserverTable.get(endpoints);
            if (null != requestObserver) {
                return requestObserver;
            }
        } finally {
            telemetryReqObserverTableLock.readLock().unlock();
        }
        telemetryReqObserverTableLock.writeLock().lock();
        try {
            requestObserver = telemetryReqObserverTable.get(endpoints);
            if (null != requestObserver) {
                return requestObserver;
            }
            requestObserver = new TelemetryRequestObserver(this, endpoints);
            telemetryReqObserverTable.put(endpoints, requestObserver);
            return requestObserver;
        } finally {
            telemetryReqObserverTableLock.writeLock().unlock();
        }
    }

    private void reportActiveSettings0(Endpoints endpoints, SettableFuture<Void> future) {
        try {
            if (!this.endpointsIsUsed(endpoints)) {
                LOGGER.info("Current endpoints is not used, forgive retries for reporting active settings, endpoints={}, clientId={}", endpoints, clientId);
            }
            final TelemetryRequestObserver requestObserver = this.getTelemetryRequestObserver(endpoints);
            final SettableFuture<Void> future0 = requestObserver.reportActiveSettings();
            final ListenableFuture<Void> futureWithTimeout = Futures.withTimeout(future0, Duration.ofSeconds(5), this.getScheduler());
            Futures.addCallback(futureWithTimeout, new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void result) {
                    future.set(null);
                }

                @Override
                public void onFailure(Throwable t) {
                    LOGGER.error("Failed to await report active settings result from remote, endpoints={}, clientId={}", endpoints, clientId, t);
                    // Repeated reporting until success.
                    getScheduler().schedule(() -> reportActiveSettings0(endpoints, future), 3, TimeUnit.SECONDS);
                }
            }, MoreExecutors.directExecutor());
        } catch (Throwable t) {
            final RuntimeException exception = new RuntimeException(t);
            future.setException(exception);
        }
    }

    private void applyPassiveSettings() {
    }

    @Override
    protected void shutDown() throws IOException {
        LOGGER.info("Begin to shutdown the rocketmq client, clientId={}", clientId);
        notifyClientTermination();
        if (null != this.updateRouteCacheFuture) {
            updateRouteCacheFuture.cancel(false);
        }
        ClientManagerFactory.getInstance().unregisterClient(namespace, this);
        LOGGER.info("Shutdown the rocketmq client, clientId={}", clientId);
    }

    private void updateRouteCache() {

    }

    public abstract NotifyClientTerminationRequest wrapNotifyClientTerminationRequest();

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

    public void close() throws IOException {
        LOGGER.info("Begin to close the rocketmq client, clientId={}", clientId);
        ClientManagerFactory.getInstance().unregisterClient(namespace, this);
        LOGGER.info("Close the rocketmq client successfully, clientId={}", clientId);
    }

    @Override
    public String getClientId() {
        return clientId;
    }

    @Override
    public void doHeartbeat() {
        final Set<Endpoints> totalEndpoints = getTotalRouteEndpoints();
        final HeartbeatRequest request = wrapHeartbeatRequest();
        for (Endpoints endpoints : totalEndpoints) {
            doHeartbeat(request, endpoints);
        }
    }

    public Metadata sign() throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeyException {
        return Signature.sign(clientConfiguration);
    }

    protected void doHeartbeat(HeartbeatRequest request, final Endpoints endpoints) {
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

    public abstract HeartbeatRequest wrapHeartbeatRequest();

    @Override
    public void doStats() {
    }

    private ListenableFuture<TopicRouteDataResult> fetchTopicRoute(final String topic) {
        final SettableFuture<TopicRouteDataResult> future = SettableFuture.create();
        try {
            Resource topicResource = Resource.newBuilder().setResourceNamespace(namespace).setName(topic).build();
            final QueryRouteRequest request = QueryRouteRequest.newBuilder().setTopic(topicResource).build();
            final Metadata metadata = sign();
            final ListenableFuture<QueryRouteResponse> responseFuture =
                clientManager.queryRoute(accessEndpoints, metadata, request, clientConfiguration.getRequestTimeout());
            return Futures.transform(responseFuture, response -> {
                final Status status = response.getStatus();
                final Code code = status.getCode();
                if (Code.OK != code) {
                    LOGGER.error("Exception raised while fetch topic route from remote, namespace={}, topic={}, " +
                            "clientId={}, accessPoint={}, code={}, status message=[{}]", namespace, topic, clientId,
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

    protected boolean endpointsIsUsed(Endpoints endpoints) {
        final Set<Endpoints> totalRouteEndpoints = getTotalRouteEndpoints();
        return totalRouteEndpoints.contains(endpoints);
    }

    private synchronized Set<Endpoints> updateTopicRouteResultCache(String topic, TopicRouteDataResult newResult) {
        final Set<Endpoints> before = getTotalRouteEndpoints();
        final TopicRouteDataResult oldResult = topicRouteResultCache.put(topic, newResult);
        if (newResult.equals(oldResult)) {
            LOGGER.info("Topic route result remains the same, namespace={}, topic={}, clientId={}", namespace, topic,
                clientId);
        } else {
            LOGGER.info("Topic route is updated, namespace={}, topic={}, clientId={}, {} => {}", namespace, topic,
                clientId, oldResult, newResult);
        }
        final Set<Endpoints> after = getTotalRouteEndpoints();
        return new HashSet<>(Sets.difference(after, before));
    }

    private void onTopicRouteDataResultUpdate(String topic, TopicRouteDataResult topicRouteDataResult) {
        final Set<Endpoints> newEndpoints = updateTopicRouteResultCache(topic, topicRouteDataResult);
        for (Endpoints endpoints : newEndpoints) {
            doTelemetry(endpoints);
        }
    }

    // TODO: not implement yet.
    private void doTelemetry(Endpoints endpoints) {

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
                        LOGGER.error("[Bug] in-flight route futures was empty, namespace={}, topic={}, clientId={}",
                            namespace, topic, clientId);
                        return;
                    }
                    LOGGER.debug("Fetch topic route successfully, namespace={}, topic={}, in-flight route future "
                        + "size={}, clientId={}", namespace, topic, newFutureSet.size(), clientId);
                    for (SettableFuture<TopicRouteDataResult> newFuture : newFutureSet) {
                        newFuture.set(result);
                    }
                } catch (Throwable t) {
                    // Should never reach here.
                    LOGGER.error("[Bug] Exception raised while update route data, namespace={}, topic={}, " +
                        "clientId={}", namespace, topic, clientId, t);
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
                        LOGGER.error("[Bug] in-flight route futures was empty, namespace={}, topic={}, clientId={}",
                            namespace, topic, clientId);
                        return;
                    }
                    LOGGER.error("Failed to fetch topic route, namespace={}, topic={}, in-flight route future " +
                        "size={}, clientId={}", namespace, topic, newFutureSet.size(), clientId, t);
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

    public String getNamespace() {
        return namespace;
    }

    public ScheduledExecutorService getScheduler() {
        return clientManager.getScheduler();
    }
}
