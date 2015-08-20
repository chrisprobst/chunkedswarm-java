package de.probst.chunkedswarm.util;

import java.io.Serializable;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 20.08.15
 */
public final class Chunk implements Serializable {

    private final long sequence;
    private final int chunkIndex;
    private final long size;

    Chunk(long sequence, int chunkIndex, long size) {
        this.sequence = sequence;
        this.chunkIndex = chunkIndex;
        this.size = size;
    }

    public long getSequence() {
        return sequence;
    }

    public int getChunkIndex() {
        return chunkIndex;
    }

    public long getSize() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Chunk chunk = (Chunk) o;

        if (sequence != chunk.sequence) return false;
        if (chunkIndex != chunk.chunkIndex) return false;
        return size == chunk.size;

    }

    @Override
    public int hashCode() {
        int result = (int) (sequence ^ (sequence >>> 32));
        result = 31 * result + chunkIndex;
        result = 31 * result + (int) (size ^ (size >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Chunk{" +
               "sequence=" + sequence +
               ", chunkIndex=" + chunkIndex +
               ", size=" + size +
               '}';
    }
}
