package de.probst.chunkedswarm.util;

import de.probst.chunkedswarm.util.later.Chunk;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 20.08.15
 */
public final class Block implements Serializable {

    private final String hash;
    private final String[] chunkHashes;
    private final long sequence;
    private final int priority;
    private final long size;
    private final Duration duration;

    public Block(String hash, String[] chunkHashes, long sequence, int priority, long size, Duration duration) {
        Objects.requireNonNull(hash);
        Objects.requireNonNull(chunkHashes);
        Objects.requireNonNull(duration);
        this.hash = hash;
        this.chunkHashes = chunkHashes;
        this.sequence = sequence;
        this.priority = priority;
        this.size = size;
        this.duration = duration;
    }

    public String getHash() {
        return hash;
    }

    public String[] getChunkHashes() {
        return chunkHashes;
    }

    public int getChunkCount() {
        return chunkHashes.length;
    }

    public long getSequence() {
        return sequence;
    }

    public int getPriority() {
        return priority;
    }

    public long getSize() {
        return size;
    }

    public Duration getDuration() {
        return duration;
    }

    public long getChunkSize(int chunkIndex) {
        if (chunkIndex < 0 || chunkIndex >= chunkHashes.length) {
            throw new IllegalArgumentException("chunkIndex < 0 || chunkIndex >= chunkHashes.length");
        }

        // Compute default chunk size (floored to previous integer)
        long chunkSize = size / chunkHashes.length;

        // Compute the size of the last chunk
        long lastChunkSize = chunkSize + (size % chunkHashes.length);

        // Return appropriate value
        return chunkIndex == chunkHashes.length - 1 ? lastChunkSize : chunkSize;
    }


    public Chunk getChunk(int chunkIndex) {
        return new Chunk(sequence, chunkIndex, getChunkSize(chunkIndex));
    }

    public Stream<Chunk> getChunks() {
        return IntStream.range(0, getChunkCount()).mapToObj(this::getChunk);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Block block = (Block) o;

        if (sequence != block.sequence) return false;
        if (priority != block.priority) return false;
        if (size != block.size) return false;
        if (!hash.equals(block.hash)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(chunkHashes, block.chunkHashes)) return false;
        return duration.equals(block.duration);

    }

    @Override
    public int hashCode() {
        int result = hash.hashCode();
        result = 31 * result + Arrays.hashCode(chunkHashes);
        result = 31 * result + (int) (sequence ^ (sequence >>> 32));
        result = 31 * result + priority;
        result = 31 * result + (int) (size ^ (size >>> 32));
        result = 31 * result + duration.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Block{" +
               "hash='" + hash + '\'' +
               ", chunkHashes=" + Arrays.toString(chunkHashes) +
               ", sequence=" + sequence +
               ", priority=" + priority +
               ", size=" + size +
               ", duration=" + duration +
               '}';
    }
}