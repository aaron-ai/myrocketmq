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

import com.google.common.collect.Sets;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.rocketmq.apis.consumer.MessageListener;
import org.apache.rocketmq.apis.message.MessageView;

public class ConsumeTask implements Callable<Collection<MessageView>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeTask.class);

    private final String clientId;
    private final MessageListener messageListener;
    private final List<MessageView> messageViews;

    public ConsumeTask(String clientId, MessageListener messageListener, List<MessageView> messageViews) {
        this.clientId = clientId;
        this.messageListener = messageListener;
        this.messageViews = messageViews;
    }

    /**
     * Invoke {@link MessageListener} to consumer message, the return value is the subset of {@link #messageViews}.
     *
     * @return message(s) which is consumed successfully.
     */
    @Override
    public Collection<MessageView> call() {
        List<MessageView> successList = new ArrayList<>();
        try {
            // Convert it into an immutable list in case of modification from user.
            final List<MessageView> immutableMessageViews = Collections.unmodifiableList(messageViews);
            messageListener.consume(immutableMessageViews, successList);
        } catch (Throwable t) {
            LOGGER.error("Message listener raised an exception while consuming messages, clientId={}", clientId, t);
        }
        // Make sure that the return value is the subset of messageViews.
        return new HashSet<>(Sets.intersection(new HashSet<>(messageViews), new HashSet<>(successList)));
    }
}
