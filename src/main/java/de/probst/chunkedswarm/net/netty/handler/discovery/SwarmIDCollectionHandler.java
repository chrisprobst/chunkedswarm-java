package de.probst.chunkedswarm.net.netty.handler.discovery;

import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIdAquisitionEvent;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIdCollectionEvent;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.SetCollectorAddressMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.SetLocalSwarmIdMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.UpdateNeighboursMessage;
import de.probst.chunkedswarm.util.SwarmId;
import de.probst.chunkedswarm.util.SwarmIdSet;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class SwarmIdCollectionHandler extends ChannelHandlerAdapter {

    // The announce address, which is announced to the registration handler
    private final SocketAddress announceAddress;

    // All known swarm ids
    private final SwarmIdSet knownSwarmIds = new SwarmIdSet();

    // The channel handler context
    private ChannelHandlerContext ctx;

    // The local swarm id
    private SwarmId localSwarmId;

    private void fireSwarmIdsCollected() {
        ctx.pipeline().fireUserEventTriggered(new SwarmIdCollectionEvent(knownSwarmIds));
    }

    private void fireSwarmIdAcquired() {
        ctx.pipeline().fireUserEventTriggered(new SwarmIdAquisitionEvent(localSwarmId));
    }

    private void setLocalSwarmId(SetLocalSwarmIdMessage setLocalSwarmIdMessage) throws Exception {
        // Safe the new swarm id
        localSwarmId = setLocalSwarmIdMessage.getLocalSwarmId();

        // Let handler chain know, that we have acquired our swarm id
        fireSwarmIdAcquired();
    }

    long d = System.currentTimeMillis();

    private void updateNeighbours(UpdateNeighboursMessage updateNeighboursMessage) {
        // Add and remove neighbours
        knownSwarmIds.get().addAll(updateNeighboursMessage.getAddNeighbours().get());
        knownSwarmIds.get().removeAll(updateNeighboursMessage.getRemoveNeighbours().get());

        if (((InetSocketAddress) announceAddress).getPort() == 20095) {
            System.out.println((System.currentTimeMillis() - d) + " Known peers: " + knownSwarmIds.get().size());

            for (SwarmId swarmId : knownSwarmIds.get()) {
                System.out.println(swarmId);
            }

            System.out.println();
            System.out.println();
        }

        // Let handler chain know, that we have updated our set of swarm ids0
        fireSwarmIdsCollected();
    }

    public SwarmIdCollectionHandler(SocketAddress announceAddress) {
        Objects.requireNonNull(announceAddress);
        this.announceAddress = announceAddress;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;

        // Send announce port to remote
        ctx.writeAndFlush(new SetCollectorAddressMessage(announceAddress))
           .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
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
