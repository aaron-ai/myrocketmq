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
import com.google.common.util.concurrent.MoreExecutors;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.rocketmq.apis.MessageQueue;
import org.apache.rocketmq.apis.consumer.ConsumeResult;
import org.apache.rocketmq.apis.consumer.MessageListener;
import org.apache.rocketmq.apis.message.MessageView;
import org.apache.rocketmq.grpcclient.route.MessageQueueImpl;

@SuppressWarnings("NullableProblems")
public class FifoConsumeService extends ConsumeService {
    private static final Logger LOGGER = LoggerFactory.getLogger(FifoConsumeService.class);

    public FifoConsumeService(String clientId, ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable,
        MessageListener messageListener, ThreadPoolExecutor consumptionExecutor, ScheduledExecutorService scheduler) {
        super(clientId, processQueueTable, messageListener, consumptionExecutor, scheduler);
    }

    @Override
    public boolean dispatch0() {
        final List<ProcessQueue> processQueues = new ArrayList<>(processQueueTable.values());
        Collections.shuffle(processQueues);
        boolean dispatched = false;
        for (final ProcessQueue pq : processQueues) {
            final Optional<MessageView> optionalMessageView = pq.tryTakeFifoMessage();
            if (!optionalMessageView.isPresent()) {
                continue;
            }
            dispatched = true;
            final MessageView messageView = optionalMessageView.get();
            LOGGER.debug("Take fifo message already, messageId={}", messageView.getMessageId());
            final ListenableFuture<ConsumeResult> future = consume(messageView);
            Futures.addCallback(future, new FutureCallback<ConsumeResult>() {
                @Override
                public void onSuccess(ConsumeResult consumeResult) {
                    pq.eraseFifoMessage(messageView, consumeResult);
                }

                @Override
                public void onFailure(Throwable t) {
                    // Should never reach here.
                    LOGGER.error("[Bug] Exception raised in consumption callback, clientId={}", clientId, t);
                }
            }, MoreExecutors.directExecutor());
        }
        return dispatched;
    }
}
