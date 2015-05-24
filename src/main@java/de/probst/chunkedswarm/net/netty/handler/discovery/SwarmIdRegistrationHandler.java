package de.probst.chunkedswarm.net.netty.handler.discovery;

import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIdEvent;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.SetCollectorAddressMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.SetLocalSwarmIdMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.UpdateNeighboursMessage;
import de.probst.chunkedswarm.net.netty.util.ChannelUtil;
import de.probst.chunkedswarm.util.SwarmId;
import de.probst.chunkedswarm.util.SwarmIdManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class SwarmIdRegistrationHandler extends ChannelHandlerAdapter {

    public static final long UPDATE_INTERVAL_MS = 1000;

    // The other, which use the same swarm id manager
    private final ChannelGroup channels;

    // The swarm id manager
    private final SwarmIdManager swarmIdManager;

    // The channel handler context
    private ChannelHandlerContext ctx;

    // The current update neighbours message
    private UpdateNeighboursMessage updateNeighboursMessage = new UpdateNeighboursMessage();

    // The update future is set, when neighbours get updated
    private ChannelFuture updateChannelFuture;

    // The local swarm id
    private volatile Optional<SwarmId> localSwarmId = Optional.empty();

    private void broadcastSwarmIdAdded() {
        if (ctx != null && localSwarmId.isPresent()) {
            channels.forEach(c -> c.pipeline().fireUserEventTriggered(SwarmIdEvent.added(this.ctx.channel(),
                                                                                         localSwarmId.get())));
        }
    }

    private void broadcastSwarmIdRemoved() {
        if (ctx != null && localSwarmId.isPresent()) {
            channels.forEach(c -> c.pipeline().fireUserEventTriggered(SwarmIdEvent.removed(this.ctx.channel(),
                                                                                           localSwarmId.get())));
        }
    }

    private void replySwarmIdAcknowledged(Channel channel) {
        Objects.requireNonNull(channel);
        if (ctx != null && localSwarmId.isPresent()) {
            channel.pipeline()
                   .fireUserEventTriggered(SwarmIdEvent.acknowledged(this.ctx.channel(), localSwarmId.get()));
        }
    }

    private void setCollectorAddress(ChannelHandlerContext ctx, SetCollectorAddressMessage setCollectorAddressMessage) {
        // We only support tcp/udp yet
        if (!(setCollectorAddressMessage.getCollectorAddress() instanceof InetSocketAddress)) {
            throw new IllegalArgumentException("!(setCollectorAddressMessage.getCollectorAddress() " +
                                               "instanceof InetSocketAddress)");
        }

        // Extract port
        int port = ((InetSocketAddress) setCollectorAddressMessage.getCollectorAddress()).getPort();

        // TODO: Cast to inet socket address, so be careful with different socket types!
        InetSocketAddress inetSocketAddress = (InetSocketAddress) ctx.channel().remoteAddress();

        // Register the new client and store local swarm id
        SwarmId newLocalSwarmId = swarmIdManager.register(new InetSocketAddress(inetSocketAddress.getAddress(), port));

        // Set the local swarm id
        localSwarmId = Optional.of(newLocalSwarmId);

        // Send the new local swarm id to the remote
        ctx.writeAndFlush(new SetLocalSwarmIdMessage(newLocalSwarmId))
           .addListener(fut -> {
               if (fut.isSuccess()) {
                   broadcastSwarmIdAdded();
               }
           })
           .addListener(ChannelUtil.REPORT_IF_FAILED_LISTENER);
    }

    private void prepareUpdateNeighboursMessage(SwarmIdEvent swarmIdEvent) {
        boolean isDifferentSwarmId = !Optional.of(swarmIdEvent.getSwarmId()).equals(localSwarmId);

        switch (swarmIdEvent.getType()) {
            case Added:
                if (isDifferentSwarmId) {
                    updateNeighboursMessage.getAddNeighbours().get().add(swarmIdEvent.getSwarmId());
                    replySwarmIdAcknowledged(swarmIdEvent.getChannel());
                }
                break;
            case Removed:
                if (isDifferentSwarmId) {
                    updateNeighboursMessage.getRemoveNeighbours().get().add(swarmIdEvent.getSwarmId());
                }
                break;
            case Acknowledged:
                updateNeighboursMessage.getAddNeighbours().get().add(swarmIdEvent.getSwarmId());
                break;
        }
    }

    private void updateNeighbours() {
        // No updates, skip this update
        if (updateNeighboursMessage.isEmpty()) {
            return;
        }

        // Pending update channel future, just ignore the current update
        if (updateChannelFuture != null) {
            return;
        }

        // Write the update neighbours message
        ctx.writeAndFlush(updateNeighboursMessage)
           .addListener(fut -> updateChannelFuture = null)
           .addListener(ChannelUtil.REPORT_IF_FAILED_LISTENER);

        // Create a new update message
        updateNeighboursMessage = new UpdateNeighboursMessage();
    }

    public SwarmIdRegistrationHandler(ChannelGroup channels, SwarmIdManager swarmIdManager) {
        Objects.requireNonNull(channels);
        Objects.requireNonNull(swarmIdManager);
        this.channels = channels;
        this.swarmIdManager = swarmIdManager;
    }

    public Optional<SwarmId> getLocalSwarmId() {
        return localSwarmId;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SwarmIdEvent) {
            prepareUpdateNeighboursMessage((SwarmIdEvent) evt);
        }

        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;

        ctx.executor()
           .scheduleAtFixedRate(this::updateNeighbours, UPDATE_INTERVAL_MS, UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        localSwarmId.ifPresent(swarmIdManager::unregister);
        broadcastSwarmIdRemoved();
        super.channelInactive(ctx);
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
