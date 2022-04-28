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

package org.apache.rocketmq.grpcclient.impl.producer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.Status;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.apis.MessageQueue;
import org.apache.rocketmq.apis.message.MessageId;
import org.apache.rocketmq.apis.producer.SendReceipt;
import org.apache.rocketmq.grpcclient.message.MessageIdCodec;

public class SendReceiptImpl implements SendReceipt {
    private final MessageId messageId;
    private final String transactionId;
    private final MessageQueue messageQueue;
    private final long offset;

    private SendReceiptImpl(MessageId messageId, String transactionId, MessageQueue messageQueue, long offset) {
        this.messageId = messageId;
        this.transactionId = transactionId;
        this.messageQueue = messageQueue;
        this.offset = offset;
    }

    @Override
    public MessageId getMessageId() {
        return messageId;
    }

    @Override
    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public String getTransactionId() {
        return transactionId;
    }

    @Override
    public long getOffset() {
        return offset;
    }

    public static List<SendReceipt> processSendResponse(MessageQueue mq, SendMessageResponse response) {
        final Status status = response.getStatus();
        final Code code = status.getCode();
        if (Code.OK != code) {
            // TODO:
            throw new RuntimeException();
        }
        List<SendReceipt> sendReceipts = new ArrayList<>();
        final List<apache.rocketmq.v2.SendReceipt> list = response.getReceiptsList();
        for (apache.rocketmq.v2.SendReceipt receipt : list) {
            final SendReceiptImpl impl = new SendReceiptImpl(MessageIdCodec.getInstance().decode(receipt.getMessageId()), receipt.getTransactionId(), mq, receipt.getOffset());
            sendReceipts.add(impl);
        }
        return sendReceipts;
    }
}
