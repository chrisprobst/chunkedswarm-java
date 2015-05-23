package de.probst.chunkedswarm.net.netty.handler.discovery;

import de.probst.chunkedswarm.net.netty.util.ChannelUtil;
import de.probst.chunkedswarm.util.SwarmId;
import de.probst.chunkedswarm.util.SwarmIdManager;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.Optional;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class SwarmIdRegistrationHandler extends ChannelHandlerAdapter {

    // The swarm id manager
    private final SwarmIdManager swarmIdManager;

    // The local swarm id
    private volatile Optional<SwarmId> localSwarmId = Optional.empty();

    private void setCollectorAddress(ChannelHandlerContext ctx, SetCollectorAddressMessage setCollectorAddressMessage) {
        // We only support tcp/udp yet
        if (!(setCollectorAddressMessage.getCollectorAddress() instanceof InetSocketAddress)) {
            throw new IllegalArgumentException("!(setCollectorAddressMessage.getCollectorAddress() " +
                                               "instanceof InetSocketAddress)");
        }

        // Extract port
        int port = ((InetSocketAddress) setCollectorAddressMessage.getCollectorAddress()).getPort();

        // TODO: Cast to inet socket address, so be careful with differeny socket types!
        InetSocketAddress inetSocketAddress = (InetSocketAddress) ctx.channel().remoteAddress();

        // Register the new client and store local swarm id
        SwarmId newLocalSwarmId = swarmIdManager.register(new InetSocketAddress(inetSocketAddress.getAddress(), port));

        // Set the local swarm id
        localSwarmId = Optional.of(newLocalSwarmId);

        // Send the new local swarm id to the remote
        ctx.writeAndFlush(new SetLocalSwarmIdMessage(newLocalSwarmId))
           .addListener(ChannelUtil.REPORT_IF_FAILED_LISTENER);
    }

    public SwarmIdRegistrationHandler(SwarmIdManager swarmIdManager) {
        Objects.requireNonNull(swarmIdManager);
        this.swarmIdManager = swarmIdManager;
    }

    public Optional<SwarmId> getLocalSwarmId() {
        return localSwarmId;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!localSwarmId.isPresent() && !(msg instanceof SetCollectorAddressMessage)) {
            throw new IllegalStateException("!localSwarmId.isPresent() && !(msg instanceof SetCollectorAddressMessage)");
        } else if (!localSwarmId.isPresent() && msg instanceof SetCollectorAddressMessage) {
            setCollectorAddress(ctx, (SetCollectorAddressMessage) msg);
        } else {
            super.channelRead(ctx, msg);
        }
    }
}
