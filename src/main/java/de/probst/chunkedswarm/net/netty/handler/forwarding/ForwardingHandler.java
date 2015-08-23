package de.probst.chunkedswarm.net.netty.handler.forwarding;

import de.probst.chunkedswarm.net.netty.handler.connection.event.ConnectionChangeEvent;
import de.probst.chunkedswarm.net.netty.handler.connection.event.ConnectionEvent;
import de.probst.chunkedswarm.net.netty.handler.forwarding.event.ForwardingCompletedEvent;
import de.probst.chunkedswarm.net.netty.handler.push.message.ChunkPushMessage;
import de.probst.chunkedswarm.util.SwarmID;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 21.08.15
 */
public final class ForwardingHandler extends ChannelHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(ForwardingHandler.class);

    private final Set<ForwardingTracker> pendingForwardingTrackers = new LinkedHashSet<>();
    private ChannelHandlerContext ctx;
    private Map<SwarmID, Channel> engagedOutboundChannels = Collections.emptyMap();

    private void fireForwardingCompleted(ForwardingTracker forwardingTracker) {
        ctx.pipeline().fireUserEventTriggered(new ForwardingCompletedEvent(forwardingTracker));
    }

    private void handleConnectionEvent(ConnectionEvent evt) {
        if (evt.getConnectionChangeEvent().getDirection() == ConnectionChangeEvent.Direction.Outbound) {
            engagedOutboundChannels = evt.getEngagedConnections();
        }
    }


    private void handleChunkPushMessage(ChunkPushMessage msg) {
        if (engagedOutboundChannels.isEmpty()) {
            logger.info("Nothing to forward, outbound channels empty");
            return;
        }

        ForwardingTracker forwardingTracker = ForwardingTracker.createFrom(this::fireForwardingCompleted,
                                                                           msg.getBlockHeader(),
                                                                           msg.getChunkHeader(),
                                                                           msg.getChunkPayload(),
                                                                           engagedOutboundChannels.values());

        // Add the new forwarding tracker
        pendingForwardingTrackers.add(forwardingTracker);

        // Compute statistics
        logger.info("Forwarding: " + forwardingTracker.getBlockHeader());
    }

    private void handleForwardingCompletedEvent(ForwardingCompletedEvent evt) {
        // Remove the forwarding tracker, it is not pending anymore
        ForwardingTracker forwardingTracker = evt.getForwardingTracker();
        pendingForwardingTrackers.remove(forwardingTracker);

        // Compute statistics
        long count = forwardingTracker.getChannels().size();

        // Compute statistics
        long failed = forwardingTracker.getFailedChannels().size();

        String rate = (count - failed) + "/" + count;
        logger.info("Forwarded: " + forwardingTracker.getBlockHeader() + ", Success: " + rate);

        // Log failed channels
        forwardingTracker.getFailedChannels()
                         .forEach((c, f) -> logger.warn("Partial forwardingTracker failure", f.cause()));
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        super.channelActive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof ConnectionEvent) {
            handleConnectionEvent((ConnectionEvent) evt);
        } else if (evt instanceof ForwardingCompletedEvent) {
            handleForwardingCompletedEvent((ForwardingCompletedEvent) evt);
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ChunkPushMessage) {
            handleChunkPushMessage((ChunkPushMessage) msg);
        } else {
            super.channelRead(ctx, msg);
        }
    }
}
