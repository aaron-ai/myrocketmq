package org.apache.rocketmq.grpcclient.impl;

import apache.rocketmq.v2.ApplyPassiveSettingsCommand;
import apache.rocketmq.v2.ApplyPassiveSettingsResult;
import apache.rocketmq.v2.ReportActiveSettingsResult;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.SettableFuture;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import org.apache.rocketmq.grpcclient.route.Endpoints;

public class TelemetryResponseObserver implements StreamObserver<TelemetryCommand> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TelemetryResponseObserver.class);

    private static final Duration ACTIVE_SETTINGS_COMMAND_EXPIRED_DURATION = Duration.ofSeconds(3);
    private final Cache<String/* Nonce */, SettableFuture<Void>> reportActiveSettingsFutureCache;

    private final ClientImpl impl;
    private final Endpoints endpoints;

    public TelemetryResponseObserver(ClientImpl impl, Endpoints endpoints) {
        this.reportActiveSettingsFutureCache = CacheBuilder.newBuilder().expireAfterWrite(ACTIVE_SETTINGS_COMMAND_EXPIRED_DURATION).build();
        this.impl = impl;
        this.endpoints = endpoints;
    }

    public SettableFuture<Void> registerActiveSettingsResultFuture(String nonce) {
        final SettableFuture<Void> future = SettableFuture.create();
        reportActiveSettingsFutureCache.put(nonce, future);
        return future;
    }

    @Override
    public void onNext(TelemetryCommand serverCommand) {
        switch (serverCommand.getCommandCase()) {
            case APPLY_PASSIVE_SETTINGS_COMMAND:
                final ApplyPassiveSettingsCommand command = serverCommand.getApplyPassiveSettingsCommand();
                if (!impl.startupPassiveSettingsFuture.isDone()) {
                    impl.startupPassiveSettingsFuture.set(command);
                }
                impl.handlePassiveSettingsCommand(command);
                final ApplyPassiveSettingsResult applyPassiveSettingsResult = ApplyPassiveSettingsResult.newBuilder().setNonce(command.getNonce()).build();
                final TelemetryRequestObserver requestObserver = impl.getTelemetryRequestObserver(endpoints);
                final TelemetryCommand clientCommand = TelemetryCommand.newBuilder().setApplyPassiveSettingsResult(applyPassiveSettingsResult).build();
                requestObserver.next(clientCommand);
                break;
            case REPORT_ACTIVE_SETTINGS_RESULT:
                final ReportActiveSettingsResult result = serverCommand.getReportActiveSettingsResult();
                final String nonce = result.getNonce();
                final SettableFuture<Void> future = reportActiveSettingsFutureCache.getIfPresent(nonce);
                if (null == future) {
                    LOGGER.warn("Reported active settings command future not found, may be expired, nonce={}, endpoints={}, clientId={}", nonce, endpoints, impl.clientId);
                    return;
                }
                future.set(null);
                break;
            case RECOVER_ORPHANED_TRANSACTION_COMMAND:
                break;
            case PRINT_THREAD_STACK_TRACE_COMMAND:
                break;
            case VERIFY_MESSAGE_COMMAND:
                break;
            default:
                LOGGER.warn("");
        }
    }

    @Override
    public void onError(Throwable throwable) {
        LOGGER.error("Exception raised from stream response observer, clientId={}", impl.getClientId(), throwable);

    }

    @Override
    public void onCompleted() {
    }
}
