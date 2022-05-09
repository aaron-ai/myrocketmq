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

package org.apache.rocketmq.grpcclient.impl.consumer;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.apis.consumer.ConsumeResult;
import org.apache.rocketmq.apis.consumer.MessageListener;
import org.apache.rocketmq.apis.message.MessageView;
import org.apache.rocketmq.grpcclient.misc.Dispatcher;
import org.apache.rocketmq.grpcclient.route.MessageQueueImpl;

@SuppressWarnings("NullableProblems")
public abstract class ConsumeService extends Dispatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeService.class);

    protected final ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable;

    protected final String clientId;
    // TODO
    private final int maxDeliveryAttempts;
    private final MessageListener messageListener;
    private final ThreadPoolExecutor consumptionExecutor;
    private final ScheduledExecutorService scheduler;

    public ConsumeService(String clientId, ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable, int maxDeliveryAttempts,
        MessageListener messageListener, ThreadPoolExecutor consumptionExecutor, ScheduledExecutorService scheduler) {
        super(clientId);
        this.clientId = clientId;
        this.processQueueTable = processQueueTable;
        this.maxDeliveryAttempts = maxDeliveryAttempts;
        this.messageListener = messageListener;
        this.consumptionExecutor = consumptionExecutor;
        this.scheduler = scheduler;
    }

    @Override
    public void shutDown() throws InterruptedException {
        LOGGER.info("Begin to shutdown the consume service, clientId={}", clientId);
        super.shutDown();
        LOGGER.info("Shutdown the consume service successfully, clientId={}", clientId);
    }

    /**
     * dispatch message(s) once
     *
     * @return if message is dispatched.
     */
    public abstract boolean dispatch0();

    /**
     * Loop of message dispatch.
     */
    public void dispatch() {
        boolean dispatched;
        do {
            dispatched = dispatch0();
        }
        while (dispatched);
    }

    public ListenableFuture<ConsumeResult> consume(MessageView messageView) {
        return consume(messageView, Duration.ZERO);
    }

    public ListenableFuture<ConsumeResult> consume(MessageView messageView, Duration delay) {
        final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(consumptionExecutor);
        final ConsumeTask task = new ConsumeTask(clientId, messageListener, messageView);
        // Consume message with no delay.
        if (Duration.ZERO.compareTo(delay) <= 0) {
            return executorService.submit(task);
        }
        final SettableFuture<ConsumeResult> future0 = SettableFuture.create();
        scheduler.schedule(() -> {
            final ListenableFuture<ConsumeResult> future = executorService.submit(task);
            Futures.addCallback(future, new FutureCallback<ConsumeResult>() {
                @Override
                public void onSuccess(ConsumeResult consumeResult) {
                    future0.set(consumeResult);
                }

                @Override
                public void onFailure(Throwable t) {
                    // Should never reach here.
                    LOGGER.error("[Bug] Exception raised while submitting scheduled consumption task, clientId={}",
                        clientId, t);
                }
            }, MoreExecutors.directExecutor());
        }, delay.toNanos(), TimeUnit.NANOSECONDS);
        return future0;
    }
}
