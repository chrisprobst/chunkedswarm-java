package de.probst.chunkedswarm.util;

import java.io.Serializable;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 20.08.15
 */
public final class ChunkHeader implements Serializable {

    private final int sequence;
    private final int chunkIndex;
    private final int size;

    ChunkHeader(int sequence, int chunkIndex, int size) {
        this.sequence = sequence;
        this.chunkIndex = chunkIndex;
        this.size = size;
    }

    public int getSequence() {
        return sequence;
    }

    public int getChunkIndex() {
        return chunkIndex;
    }

    public int getSize() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ChunkHeader chunk = (ChunkHeader) o;

        if (sequence != chunk.sequence) return false;
        if (chunkIndex != chunk.chunkIndex) return false;
        return size == chunk.size;

    }

    @Override
    public int hashCode() {
        int result = sequence;
        result = 31 * result + chunkIndex;
        result = 31 * result + size;
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
