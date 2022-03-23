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

package org.apache.rocketmq.grpcclient.message;

import org.apache.rocketmq.apis.message.MessageId;
import org.apache.rocketmq.grpcclient.utility.UtilAll;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageIdCodec {
    private static final MessageIdCodec INSTANCE = new MessageIdCodec();

    public static final int MESSAGE_ID_LENGTH_FOR_V1_OR_LATER = 34;

    public static final String MESSAGE_ID_VERSION_V0 = "00";
    public static final String MESSAGE_ID_VERSION_V1 = "01";

    private final String processFixedStringV1;
    private final long secondsSinceCustomEpoch;
    private final long secondsStartTimestamp;
    private long seconds;
    private final AtomicInteger sequence;

    private MessageIdCodec() {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.BIG_ENDIAN);

        byte[] prefix0 = UtilAll.macAddress();
        buffer.put(prefix0, 0, 6);

        ByteBuffer pidBuffer = ByteBuffer.allocate(4);
        pidBuffer.order(ByteOrder.BIG_ENDIAN);
        final int pid = UtilAll.processId();
        pidBuffer.putInt(pid);

        // Copy the lower 2 bytes
        buffer.put(pidBuffer.array(), 2, 2);

        buffer.flip();
        processFixedStringV1 = UtilAll.encodeHexString(buffer, false);

        secondsSinceCustomEpoch = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - customEpochMillis());
        secondsStartTimestamp = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
        seconds = deltaSeconds();

        sequence = new AtomicInteger(0);
    }

    public static MessageIdCodec getInstance() {
        return INSTANCE;
    }

    private long customEpochMillis() {
        // 2021-01-01 00:00:00
        final Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, 2021);
        calendar.set(Calendar.MONTH, Calendar.JANUARY);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        return calendar.getTime().getTime();
    }

    private long deltaSeconds() {
        return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime()) - secondsStartTimestamp + secondsSinceCustomEpoch;
    }

    public MessageId nextMessageId() {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.BIG_ENDIAN);

        final ByteBuffer deltaSecondsBuffer = ByteBuffer.allocate(8);
        deltaSecondsBuffer.order(ByteOrder.BIG_ENDIAN);
        final long deltaSeconds = deltaSeconds();
        if (seconds != deltaSeconds) {
            seconds = deltaSeconds;
        }
        deltaSecondsBuffer.putLong(seconds);
        buffer.put(deltaSecondsBuffer.array(), 4, 4);
        buffer.putInt(sequence.getAndIncrement());

        buffer.flip();
        final String suffix = processFixedStringV1 + UtilAll.encodeHexString(buffer, false);
        return new MessageIdImpl(MESSAGE_ID_VERSION_V1, suffix);
    }

    public MessageId decode(String messageId) {
        if (MESSAGE_ID_LENGTH_FOR_V1_OR_LATER != messageId.length()) {
            return new MessageIdImpl(MESSAGE_ID_VERSION_V0, messageId);
        }
        return new MessageIdImpl(messageId.substring(0, 2), messageId.substring(2));
    }
}
