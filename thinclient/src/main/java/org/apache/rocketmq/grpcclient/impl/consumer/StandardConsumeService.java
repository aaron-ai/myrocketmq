package org.apache.rocketmq.grpcclient.impl.consumer;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.rocketmq.apis.MessageQueue;
import org.apache.rocketmq.apis.consumer.MessageListener;
import org.apache.rocketmq.apis.message.MessageView;
import org.apache.rocketmq.grpcclient.route.MessageQueueImpl;

@SuppressWarnings(value = {"UnstableApiUsage", "NullableProblems"})
public class StandardConsumeService extends ConsumeService {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardConsumeService.class);

    private final int maxBatchSize;

    public StandardConsumeService(String clientId, ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable,
        MessageListener messageListener, ThreadPoolExecutor consumptionExecutor, ScheduledExecutorService scheduler,
        int maxBatchSize) {
        super(clientId, processQueueTable, messageListener, consumptionExecutor, scheduler);
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public boolean dispatch0() {
        final List<ProcessQueue> processQueues = new ArrayList<>(processQueueTable.values());
        // Shuffle all process queue in case messages are always consumed firstly in one message queue.
        Collections.shuffle(processQueues);

        int accumulativeSize = 0;

        final Map<MessageQueue, List<MessageView>> messageViewTable = new HashMap<>();
        // Iterate all process queues to submit consumption task.
        for (ProcessQueue pq : processQueues) {
            List<MessageView> messageViews = pq.tryTakeMessages(maxBatchSize - accumulativeSize);
            if (messageViews.isEmpty()) {
                continue;
            }
            final MessageQueue mq = pq.getMessageQueue();
            // Add message list to message table.
            messageViewTable.put(mq, messageViews);
            accumulativeSize += messageViews.size();
            if (accumulativeSize >= maxBatchSize) {
                break;
            }
        }
        // Aggregate all messages into list.
        List<MessageView> messageViews = new ArrayList<>();
        for (List<MessageView> list : messageViewTable.values()) {
            messageViews.addAll(list);
        }

        // No new message arrived for current round.
        if (messageViews.isEmpty()) {
            return false;
        }

        final ListenableFuture<Collection<MessageView>> future = consume(messageViews);
        Futures.addCallback(future, new FutureCallback<Collection<MessageView>>() {
            @Override
            public void onSuccess(Collection<MessageView> successList) {
                for (Map.Entry<MessageQueue, List<MessageView>> entry : messageViewTable.entrySet()) {
                    final MessageQueue mq = entry.getKey();
                    final ProcessQueue pq = processQueueTable.get(mq);
                    if (null == pq) {
                        // TODO
                        continue;
                    }
                    if (!successList.isEmpty()) {
                        pq.eraseMessages(successList, true);
                    }
                    final List<MessageView> totalList = entry.getValue();
                    if (totalList.size() == successList.size()) {
                        // All message(s) is consumed successfully.
                        return;
                    }
                    final HashSet<MessageView> totalSet = new HashSet<>(totalList);
                    final HashSet<MessageView> successSet = new HashSet<>(successList);
                    final ArrayList<MessageView> failureList = new ArrayList<>(Sets.difference(totalSet, successSet));
                    if (!failureList.isEmpty()) {
                        pq.eraseMessages(failureList, false);
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                // should never reach here.
                LOGGER.error("[Bug] Exception raised in consumption callback, clientId={}", clientId, t);
            }
        });
        return true;
    }
}
