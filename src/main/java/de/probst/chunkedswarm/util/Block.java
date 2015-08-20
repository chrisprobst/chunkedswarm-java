package de.probst.chunkedswarm.util;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 20.08.15
 */
public final class Block implements Serializable {

    private final String hash;
    private final List<String> chunkHashes;
    private final long sequence;
    private final int priority;
    private final long size;
    private final Duration duration;

    public Block(Block block, List<String> chunkHashes) {
        this(block.hash, chunkHashes, block.sequence, block.priority, block.size, block.duration);
    }

    public Block(String hash, long sequence, int priority, long size, Duration duration) {
        this(hash, Collections.singletonList(hash), sequence, priority, size, duration);
    }

    public Block(String hash, List<String> chunkHashes, long sequence, int priority, long size, Duration duration) {
        Objects.requireNonNull(hash);
        Objects.requireNonNull(chunkHashes);
        Objects.requireNonNull(duration);
        this.hash = hash;
        this.chunkHashes = Collections.unmodifiableList(chunkHashes);
        this.sequence = sequence;
        this.priority = priority;
        this.size = size;
        this.duration = duration;
    }

    public String getHash() {
        return hash;
    }

    public List<String> getChunkHashes() {
        return chunkHashes;
    }

    public int getChunkCount() {
        return chunkHashes.size();
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
        if (chunkIndex < 0 || chunkIndex >= chunkHashes.size()) {
            throw new IllegalArgumentException("chunkIndex < 0 || chunkIndex >= chunkHashes.size()");
        }

        // Compute default chunk size (floored to previous integer)
        long chunkSize = size / chunkHashes.size();

        // Compute the size of the last chunk
        long lastChunkSize = chunkSize + (size % chunkHashes.size());

        // Return appropriate value
        return chunkIndex == chunkHashes.size() - 1 ? lastChunkSize : chunkSize;
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
        if (!chunkHashes.equals(block.chunkHashes)) return false;
        return duration.equals(block.duration);

    }

    @Override
    public int hashCode() {
        int result = hash.hashCode();
        result = 31 * result + chunkHashes.hashCode();
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
               ", chunkHashes=" + chunkHashes +
               ", sequence=" + sequence +
               ", priority=" + priority +
               ", size=" + size +
               ", duration=" + duration +
               '}';
    }
}