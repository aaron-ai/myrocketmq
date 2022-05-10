package org.apache.rocketmq.grpcclient.impl;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClientManagerRegistryTest {
    @Mock
    private ClientManagerImpl clientManager;

    @InjectMocks
    private ClientManagerRegistry clientManagerRegistry = ClientManagerRegistry.getInstance();

    @Test
    public void xx() {
//        clientManagerRegistry.registerClient(null);
    }
}
