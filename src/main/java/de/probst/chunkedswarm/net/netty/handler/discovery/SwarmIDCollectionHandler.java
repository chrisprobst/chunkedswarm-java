package de.probst.chunkedswarm.net.netty.handler.discovery;

import de.probst.chunkedswarm.net.netty.util.ChannelFutureListeners;
import de.probst.chunkedswarm.util.SwarmId;
import de.probst.chunkedswarm.util.SwarmIdSet;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.net.SocketAddress;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class SwarmIdCollectionHandler extends ChannelHandlerAdapter {

    // The announce address, which is announced to the registration handler
    private final SocketAddress announceAddress;

    // The local swarm id
    private volatile SwarmId localSwarmId;

    private void readLocalSwarmId(SwarmId swarmId) throws Exception {
        // Safe the new swarm id
        localSwarmId = swarmId;

        // TODO: Verify ?
        System.out.println("Got new swarm id: " + localSwarmId);
    }

    private void readNeighbours(SwarmIdSet swarmIdSet) {

    }

    public SwarmIdCollectionHandler(SocketAddress announceAddress) {
        Objects.requireNonNull(announceAddress);
        this.announceAddress = announceAddress;
    }

    public SwarmId getLocalSwarmId() {
        return localSwarmId;
    }

    public boolean hasLocalSwarmId() {
        return localSwarmId != null;
    }

    public SocketAddress getAnnounceAddress() {
        return announceAddress;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // Send announce port to remote
        ctx.writeAndFlush(announceAddress).addListener(ChannelFutureListeners.REPORT_IF_FAILED);
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (localSwarmId == null && !(msg instanceof SwarmId)) {
            throw new IllegalStateException("localSwarmId == null && !(msg instanceof SwarmId)");
        } else if (localSwarmId == null && msg instanceof SwarmId) {
            readLocalSwarmId((SwarmId) msg);
        } else if (msg instanceof SwarmIdSet) {
            readNeighbours((SwarmIdSet) msg);
        } else {
            super.channelRead(ctx, msg);
        }
    }
}
