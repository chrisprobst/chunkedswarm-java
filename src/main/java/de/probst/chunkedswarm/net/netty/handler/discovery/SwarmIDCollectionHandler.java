package de.probst.chunkedswarm.net.netty.handler.discovery;

import de.probst.chunkedswarm.net.netty.handler.discovery.message.SetCollectorAddressMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.SetLocalSwarmIdMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.UpdateNeighboursMessage;
import de.probst.chunkedswarm.net.netty.util.ChannelUtil;
import de.probst.chunkedswarm.util.SwarmId;
import de.probst.chunkedswarm.util.SwarmIdSet;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.Optional;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class SwarmIdCollectionHandler extends ChannelHandlerAdapter {

    // The announce address, which is announced to the registration handler
    private final SocketAddress announceAddress;

    // All known swarm ids
    private final SwarmIdSet knownSwarmIds = new SwarmIdSet();

    // The local swarm id
    private volatile Optional<SwarmId> localSwarmId = Optional.empty();

    private void setLocalSwarmId(SetLocalSwarmIdMessage setLocalSwarmIdMessage) throws Exception {
        // Safe the new swarm id
        localSwarmId = Optional.of(setLocalSwarmIdMessage.getLocalSwarmId());
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
    }

    public SwarmIdCollectionHandler(SocketAddress announceAddress) {
        Objects.requireNonNull(announceAddress);
        this.announceAddress = announceAddress;
    }

    public Optional<SwarmId> getLocalSwarmId() {
        return localSwarmId;
    }

    public SocketAddress getAnnounceAddress() {
        return announceAddress;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // Send announce port to remote
        ctx.writeAndFlush(new SetCollectorAddressMessage(announceAddress))
           .addListener(ChannelUtil.CLOSE_IF_FAILED_LISTENER);
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!localSwarmId.isPresent() && !(msg instanceof SetLocalSwarmIdMessage)) {
            throw new IllegalStateException("!localSwarmId.isPresent() && !(msg instanceof SetLocalSwarmIdMessage)");
        } else if (!localSwarmId.isPresent() && msg instanceof SetLocalSwarmIdMessage) {
            setLocalSwarmId((SetLocalSwarmIdMessage) msg);
        } else if (msg instanceof UpdateNeighboursMessage) {
            updateNeighbours((UpdateNeighboursMessage) msg);
        } else {
            super.channelRead(ctx, msg);
        }
    }
}
