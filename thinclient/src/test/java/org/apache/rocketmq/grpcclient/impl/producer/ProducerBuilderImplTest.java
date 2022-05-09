package org.apache.rocketmq.grpcclient.impl.producer;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.ClientServiceProvider;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.producer.Producer;
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
    public void testSetTopicWithTooLong() {
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

    @Test
    public void testBuildWithoutTopic() throws ClientException, IOException {
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder().setAccessPoint("127.0.0.1:80").build();
        Producer producer = ClientServiceProvider.loadService().newProducerBuilder().setClientConfiguration(clientConfiguration).build();
        producer.close();
    }
}
