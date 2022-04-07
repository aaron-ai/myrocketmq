package org.apache.rocketmq.grpcclient.impl.consumer;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import org.apache.rocketmq.apis.consumer.MessageListener;
import org.apache.rocketmq.apis.message.MessageView;

@SuppressWarnings(value = {"UnstableApiUsage", "NullableProblems"})
public class FifoConsumeService extends ConsumeService {
    private static final Logger LOGGER = LoggerFactory.getLogger(FifoConsumeService.class);

    public FifoConsumeService(String clientId, ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable,
        MessageListener messageListener, ThreadPoolExecutor consumptionExecutor, ScheduledExecutorService scheduler) {
        super(clientId, processQueueTable, messageListener, consumptionExecutor, scheduler);
    }

    @Override
    public boolean dispatch0() {
        final List<ProcessQueue> processQueues = new ArrayList<>(processQueueTable.values());
        Collections.shuffle(processQueues);
        boolean dispatched = false;
        for (final ProcessQueue pq : processQueues) {
            final Optional<MessageView> messageView = pq.tryTakeFifoMessage();
            if (!messageView.isPresent()) {
                continue;
            }
            dispatched = true;
            LOGGER.debug("Take fifo message already, messageId={}", messageView.get().getMessageId());
            final ListenableFuture<Boolean> future = consume(messageView.get());
            Futures.addCallback(future, new FutureCallback<Boolean>() {
                @Override
                public void onSuccess(Boolean success) {
                    pq.eraseFifoMessage(messageView.get(), success);
                }

                @Override
                public void onFailure(Throwable t) {
                    // Should never reach here.
                    LOGGER.error("[Bug] Exception raised in consumption callback, clientId={}", clientId, t);
                }
            });
        }
        return dispatched;
    }
}
