package org.apache.rocketmq.grpcclient.message;

import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SystemProperties;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.util.Optional;
import org.apache.rocketmq.apis.message.Message;
import org.apache.rocketmq.apis.message.MessageId;
import org.apache.rocketmq.grpcclient.utility.UtilAll;

/**
 * This class is a publishing view for message, which could be considered as an extension of {@link MessageImpl}.
 * Specifically speaking, Some work has been brought forward, e.g. message body compression, message id generation, etc.
 */
public class PublishingMessageImpl extends MessageImpl {
    /**
     * If message body size exceeds the threshold, it would be compressed for convenience of transport.
     */
    private static final int MESSAGE_COMPRESSION_THRESHOLD_BYTES = 1024 * 4;

    /**
     * The default GZIP compression level for message body.
     */
    private static final int MESSAGE_GZIP_COMPRESSION_LEVEL = 5;

    private final String namespace;
    private final Encoding encoding;
    private final byte[] compressedBody;
    private final MessageId messageId;
    private final MessageType messageType;
    private volatile String traceContext;

    public PublishingMessageImpl(String namespace, Message message, boolean transactionEnabled) throws IOException {
        super(message);
        this.namespace = namespace;
        this.traceContext = null;
        final byte[] body = message.getBody().array();
        // Message body length exceeds the compression threshold, try to compress it.
        if (body.length > MESSAGE_COMPRESSION_THRESHOLD_BYTES) {
            this.compressedBody = UtilAll.compressBytesGzip(body, MESSAGE_GZIP_COMPRESSION_LEVEL);
            this.encoding = Encoding.GZIP;
        } else {
            this.compressedBody = null;
            this.encoding = Encoding.IDENTITY;
        }
        this.messageId = MessageIdCodec.getInstance().nextMessageId();
        // Normal message.
        if (!message.getMessageGroup().isPresent() &&
            !message.getDeliveryTimestamp().isPresent() && !transactionEnabled) {
            messageType = MessageType.NORMAL;
            return;
        }
        // Fifo message.
        if (message.getMessageGroup().isPresent() && !transactionEnabled) {
            messageType = MessageType.FIFO;
            return;
        }
        // Delay message.
        if (message.getDeliveryTimestamp().isPresent() && !transactionEnabled) {
            messageType = MessageType.DELAY;
            return;
        }
        // Transaction message.
        if (message.getMessageGroup().isPresent() &&
            message.getDeliveryTimestamp().isPresent() && transactionEnabled) {
            messageType = MessageType.TRANSACTION;
            return;
        }
        // Transaction semantics is conflicted with fifo/delay.
        throw new IllegalArgumentException("Transactional message should not set messageGroup or deliveryTimestamp");
    }

    public MessageId getMessageId() {
        return messageId;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public byte[] getTransportBody() {
        return null == compressedBody ? getBody().array() : compressedBody;
    }

    public void setTraceContext(String traceContext) {
        this.traceContext = traceContext;
    }

    public Optional<String> getTraceContext() {
        return null == traceContext ? Optional.empty() : Optional.of(traceContext);
    }

    /**
     * Convert {@link PublishingMessageImpl} to protocol buffer.
     *
     * <p>This method should be invoked before each message sending, because the born time is reset before each
     * invocation, which means that it should not be invoked ahead of time.
     */
    public apache.rocketmq.v2.Message toProtobuf() {
        final apache.rocketmq.v2.SystemProperties.Builder systemPropertiesBuilder =
            apache.rocketmq.v2.SystemProperties.newBuilder()
                // Message keys
                .addAllKeys(keys)
                // Message Id
                .setMessageId(messageId.toString())
                // Born time should be reset before each sending
                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                // Born host
                .setBornHost(UtilAll.hostName())
                // Body encoding
                .setBodyEncoding(Encoding.toProtobuf(encoding))
                // Message type
                .setMessageType(MessageType.toProtobuf(messageType));
        // Message tag
        this.getTag().ifPresent(systemPropertiesBuilder::setTag);
        // Trace context
        this.getTraceContext().ifPresent(systemPropertiesBuilder::setTraceContext);
        final SystemProperties systemProperties = systemPropertiesBuilder.build();
        Resource topicResource = Resource.newBuilder().setResourceNamespace(namespace).setName(getTopic()).build();
        return apache.rocketmq.v2.Message.newBuilder()
            // Topic
            .setTopic(topicResource)
            // Message body
            .setBody(ByteString.copyFrom(getTransportBody()))
            // System properties
            .setSystemProperties(systemProperties)
            // User properties
            .putAllUserProperties(getProperties())
            .build();
    }
}
