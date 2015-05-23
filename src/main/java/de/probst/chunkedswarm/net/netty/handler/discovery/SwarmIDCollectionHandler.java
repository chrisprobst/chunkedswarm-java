package de.probst.chunkedswarm.net.netty.handler.discovery;

import de.probst.chunkedswarm.net.netty.handler.discovery.message.SetCollectorAddressMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.SetLocalSwarmIdMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.UpdateNeighboursMessage;
import de.probst.chunkedswarm.net.netty.util.ChannelUtil;
import de.probst.chunkedswarm.util.SwarmId;
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

    private void setLocalSwarmId(SetLocalSwarmIdMessage setLocalSwarmIdMessage) throws Exception {
        // Safe the new swarm id
        localSwarmId = setLocalSwarmIdMessage.getLocalSwarmId();

        // TODO: Verify ?
        System.out.println("Got new swarm id: " + localSwarmId);
    }

    private void updateNeighbours(UpdateNeighboursMessage updateNeighboursMessage) {

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
        ctx.writeAndFlush(new SetCollectorAddressMessage(announceAddress))
           .addListener(ChannelUtil.REPORT_IF_FAILED_LISTENER);
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (localSwarmId == null && !(msg instanceof SetLocalSwarmIdMessage)) {
            throw new IllegalStateException("localSwarmId == null && !(msg instanceof SetLocalSwarmIdMessage)");
        } else if (localSwarmId == null && msg instanceof SetLocalSwarmIdMessage) {
            setLocalSwarmId((SetLocalSwarmIdMessage) msg);
        } else if (msg instanceof UpdateNeighboursMessage) {
            updateNeighbours((UpdateNeighboursMessage) msg);
        } else {
            super.channelRead(ctx, msg);
        }
    }
}
