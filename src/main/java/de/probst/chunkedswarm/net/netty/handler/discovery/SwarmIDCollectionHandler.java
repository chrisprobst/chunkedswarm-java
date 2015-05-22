package de.probst.chunkedswarm.net.netty.handler.discovery;

import de.probst.chunkedswarm.net.netty.util.ChannelFutureListeners;
import de.probst.chunkedswarm.util.SwarmId;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class SwarmIdCollectionHandler extends ChannelHandlerAdapter {

    // The announce port, which is announced to the registration handler
    private final int announcePort;

    // The local swarm id
    private volatile SwarmId localSwarmId;

    public SwarmIdCollectionHandler(int announcePort) {
        this.announcePort = announcePort;
    }

    public SwarmId getLocalSwarmId() {
        return localSwarmId;
    }

    public boolean hasLocalSwarmId() {
        return localSwarmId != null;
    }

    public int getAnnouncePort() {
        return announcePort;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // Send announce port to remote
        ctx.writeAndFlush(announcePort).addListener(ChannelFutureListeners.REPORT_IF_FAILED);
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (localSwarmId == null) {
            if (!(msg instanceof SwarmId)) {
                throw new IllegalStateException("!(msg instanceof SwarmId)");
            }

            // TODO: Verify ?
            // Safe the new swarm id
            localSwarmId = (SwarmId) msg;

            System.out.println("Got new swarm id: " + localSwarmId);
        } else {
            super.channelRead(ctx, msg);
        }
    }
}
