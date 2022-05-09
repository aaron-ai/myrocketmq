package org.apache.rocketmq.grpcclient.impl.producer;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

public class ProducerBuilderImplTest {

    @Test(expected = NullPointerException.class)
    public void testSetClientConfigurationWithNull() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setClientConfiguration(null);
    }

    @SuppressWarnings("ConfusingArgumentToVarargsMethod")
    @Test(expected = NullPointerException.class)
    public void testSetTopicWithNull() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setTopics(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetTopicWithTooLongTopic() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        String tooLongTopic = StringUtils.repeat("a", 128);
        builder.setTopics(tooLongTopic);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNegativeAsyncThreadCount() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setSendAsyncThreadCount(-1);
    }

    @Test(expected = NullPointerException.class)
    public void testSetRetryPolicyWithNull() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setRetryPolicy(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetTransactionCheckerWithNull() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setTransactionChecker(null);
    }

    @Test(expected = NullPointerException.class)
    public void testBuildWithoutClientConfiguration() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.build();
    }
}
