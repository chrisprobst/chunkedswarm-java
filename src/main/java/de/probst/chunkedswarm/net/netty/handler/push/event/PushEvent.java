package de.probst.chunkedswarm.net.netty.handler.push.event;

import de.probst.chunkedswarm.util.Block;

import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 18.08.15
 */
public final class PushEvent {

    private final Block block;

    public PushEvent(Block block) {
        Objects.requireNonNull(block);
        this.block = block;
    }

    public Block getBlock() {
        return block;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PushEvent pushEvent = (PushEvent) o;

        return block.equals(pushEvent.block);

    }

    @Override
    public int hashCode() {
        return block.hashCode();
    }

    @Override
    public String toString() {
        return "PushEvent{" +
               "block=" + block +
               '}';
    }
}
