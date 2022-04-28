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
import org.apache.rocketmq.apis.consumer.ConsumeResult;
import org.apache.rocketmq.apis.consumer.MessageListener;
import org.apache.rocketmq.apis.message.MessageView;
import org.apache.rocketmq.grpcclient.route.MessageQueueImpl;

@SuppressWarnings("NullableProblems")
public class StandardConsumeService extends ConsumeService {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardConsumeService.class);

    public StandardConsumeService(String clientId, ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable,
        MessageListener messageListener, ThreadPoolExecutor consumptionExecutor, ScheduledExecutorService scheduler) {
        super(clientId, processQueueTable, messageListener, consumptionExecutor, scheduler);
    }

    @Override
    public boolean dispatch0() {
        final List<ProcessQueue> processQueues = new ArrayList<>(processQueueTable.values());
        // Shuffle all process queue in case messages are always consumed firstly in one message queue.
        Collections.shuffle(processQueues);
        boolean dispatched = false;
        // Iterate all process queues to submit consumption task.
        for (ProcessQueue pq : processQueues) {
            final Optional<MessageView> optionalMessageView = pq.tryTakeFifoMessage();
            if (!optionalMessageView.isPresent()) {
                continue;
            }
            dispatched = true;
            MessageView messageView = optionalMessageView.get();
            final ListenableFuture<ConsumeResult> future = consume(messageView);
            Futures.addCallback(future, new FutureCallback<ConsumeResult>() {
                @Override
                public void onSuccess(ConsumeResult consumeResult) {
                    pq.eraseMessage(messageView, consumeResult);
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
