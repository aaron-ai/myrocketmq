package org.apache.rocketmq.grpcclient.impl.consumer;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.apis.MessageQueue;
import org.apache.rocketmq.apis.consumer.MessageListener;
import org.apache.rocketmq.apis.message.MessageView;
import org.apache.rocketmq.grpcclient.misc.Dispatcher;
import org.apache.rocketmq.grpcclient.route.MessageQueueImpl;

@SuppressWarnings(value = {"UnstableApiUsage", "NullableProblems"})
public abstract class ConsumeService extends Dispatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeService.class);

    protected final ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable;

    protected final String clientId;
    private final MessageListener messageListener;
    private final ThreadPoolExecutor consumptionExecutor;
    private final ScheduledExecutorService scheduler;

    public ConsumeService(String clientId, ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable,
        MessageListener messageListener, ThreadPoolExecutor consumptionExecutor, ScheduledExecutorService scheduler) {
        this.clientId = clientId;
        this.processQueueTable = processQueueTable;
        this.messageListener = messageListener;
        this.consumptionExecutor = consumptionExecutor;
        this.scheduler = scheduler;
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Begin to shutdown the consume service, clientId={}", clientId);
        super.close();
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

    public ListenableFuture<Boolean> consume(MessageView messageView) {
        final ListenableFuture<Collection<MessageView>> future = consume(Collections.singletonList(messageView));
        return Futures.transform(future, (Function<Collection<MessageView>, Boolean>)
            messageViews -> !messageViews.isEmpty());
    }

    public ListenableFuture<Boolean> consume(MessageView messageView, Duration delay) {
        final ListenableFuture<Collection<MessageView>> future = consume(Collections.singletonList(messageView), delay);
        return Futures.transform(future, (Function<Collection<MessageView>, Boolean>)
            messageViews -> !messageViews.isEmpty());
    }

    public ListenableFuture<Collection<MessageView>> consume(List<MessageView> messageViews) {
        return consume(messageViews, Duration.ZERO);
    }

    public ListenableFuture<Collection<MessageView>> consume(List<MessageView> messageViews, Duration delay) {
        final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(consumptionExecutor);
        final ConsumeTask task = new ConsumeTask(clientId, messageListener, messageViews);
        // Consume message with no delay.
        if (Duration.ZERO.compareTo(delay) <= 0) {
            return executorService.submit(task);
        }
        final SettableFuture<Collection<MessageView>> future0 = SettableFuture.create();
        scheduler.schedule(() -> {
            final ListenableFuture<Collection<MessageView>> future = executorService.submit(task);
            Futures.addCallback(future, new FutureCallback<Collection<MessageView>>() {
                @Override
                public void onSuccess(Collection<MessageView> successCollection) {
                    future0.set(successCollection);
                }

                @Override
                public void onFailure(Throwable t) {
                    // Should never reach here.
                    LOGGER.error("[Bug] Exception raised while submitting scheduled consumption task, clientId={}",
                        clientId, t);
                }
            });
        }, delay.getNano(), TimeUnit.NANOSECONDS);
        return future0;
    }
}
