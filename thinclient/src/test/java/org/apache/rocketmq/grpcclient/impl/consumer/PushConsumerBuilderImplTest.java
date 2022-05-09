package org.apache.rocketmq.grpcclient.impl.consumer;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

public class PushConsumerBuilderImplTest {

    @Test(expected = NullPointerException.class)
    public void testSetClientConfigurationWithNull() {
        final PushConsumerBuilderImpl builder = new PushConsumerBuilderImpl();
        builder.setClientConfiguration(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetConsumerGroupWithNull() {
        final PushConsumerBuilderImpl builder = new PushConsumerBuilderImpl();
        builder.setConsumerGroup(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetConsumerGroupWithTooLong() {
        final PushConsumerBuilderImpl builder = new PushConsumerBuilderImpl();
        String tooLongConsumerGroup = StringUtils.repeat("a", 256);
        builder.setConsumerGroup(tooLongConsumerGroup);
    }

    @Test(expected = NullPointerException.class)
    public void testSetMessageListenerWithNull() {
        final PushConsumerBuilderImpl builder = new PushConsumerBuilderImpl();
        builder.setMessageListener(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNegativeMaxCacheMessageCount() {
        final PushConsumerBuilderImpl builder = new PushConsumerBuilderImpl();
        builder.setMaxCacheMessageCount(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNegativeMaxCacheMessageSizeInBytes() {
        final PushConsumerBuilderImpl builder = new PushConsumerBuilderImpl();
        builder.setMaxCacheMessageSizeInBytes(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNegativeConsumptionThreadCount() {
        final PushConsumerBuilderImpl builder = new PushConsumerBuilderImpl();
        builder.setConsumptionThreadCount(-1);
    }
}