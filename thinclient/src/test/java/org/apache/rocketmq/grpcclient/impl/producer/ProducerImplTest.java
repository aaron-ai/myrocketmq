package org.apache.rocketmq.grpcclient.impl.producer;

import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.Permission;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.apis.retry.ExponentialBackoffRetryPolicy;
import org.apache.rocketmq.grpcclient.impl.ClientManagerImpl;
import org.apache.rocketmq.grpcclient.impl.ClientManagerRegistry;
import org.apache.rocketmq.grpcclient.impl.TelemetryRespObserver;
import org.apache.rocketmq.grpcclient.route.Endpoints;
import org.apache.rocketmq.grpcclient.tool.TestBase;
import org.apache.rocketmq.grpcclient.utility.ThreadFactoryImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProducerImplTest extends TestBase {
    @Mock
    private ClientManagerImpl clientManager;

    @Mock
    private StreamObserver<TelemetryCommand> telemetryRequestObserver;

    @SuppressWarnings("unused")
    @InjectMocks
    private ClientManagerRegistry clientManagerRegistry = ClientManagerRegistry.getInstance();

    private final ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder().setAccessPoint(FAKE_ACCESS_POINT).build();
    private final String topic = FAKE_TOPIC_0;
    private final String[] str = {topic};
    private final Set<String> set = new HashSet<>(Arrays.asList(str));
    private final ExponentialBackoffRetryPolicy retryPolicy = ExponentialBackoffRetryPolicy.newBuilder().build();

    @InjectMocks
    private final ProducerImpl producer = new ProducerImpl(clientConfiguration, set, 1, retryPolicy, null);

    @Test
    public void testStartAndShutdown() throws ClientException {
        SettableFuture<QueryRouteResponse> future0 = SettableFuture.create();
        Status status = Status.newBuilder().setCode(Code.OK).build();
        List<MessageQueue> messageQueueList = new ArrayList<>();
        MessageQueue mq = MessageQueue.newBuilder().setTopic(Resource.newBuilder().setName(topic))
            .setPermission(Permission.READ_WRITE)
            .setBroker(Broker.newBuilder().setName(FAKE_BROKER_NAME_0).setEndpoints(fakePbEndpoints0())).setId(0).build();
        messageQueueList.add(mq);
        QueryRouteResponse response = QueryRouteResponse.newBuilder().setStatus(status).addAllMessageQueues(messageQueueList).build();
        future0.set(response);
        when(clientManager.queryRoute(any(Endpoints.class), any(Metadata.class), any(QueryRouteRequest.class), any(Duration.class)))
            .thenReturn(future0);
        when(clientManager.telemetry(any(Endpoints.class), any(Metadata.class), any(Duration.class), any(TelemetryRespObserver.class)))
            .thenReturn(telemetryRequestObserver);
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("TestScheduler"));
        when(clientManager.getScheduler()).thenReturn(scheduler);
        doNothing().when(telemetryRequestObserver).onNext(any(TelemetryCommand.class));
        Publishing publishing = Publishing.newBuilder().build();
        Settings settings = Settings.newBuilder().setPublishing(publishing).build();
        final Service service = producer.startAsync();
        producer.getProducerSettings().applySettings(settings);
        service.awaitRunning();
        final Service clientManagerService = mock(Service.class);
        when(clientManager.stopAsync()).thenReturn(clientManagerService);
        doNothing().when(clientManagerService).awaitTerminated();
        producer.stopAsync().awaitTerminated();
    }
}