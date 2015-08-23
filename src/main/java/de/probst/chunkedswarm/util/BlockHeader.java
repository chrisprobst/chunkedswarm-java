package de.probst.chunkedswarm.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 20.08.15
 */
public final class BlockHeader implements Serializable {

    public static int computeDefaultChunkSize(int size, int chunks) {
        return size / chunks;
    }

    public static int computeChunkSize(int size, int chunks, int chunkIndex) {
        if (chunkIndex < 0 || chunkIndex >= chunks) {
            throw new IllegalArgumentException("chunkIndex < 0 || chunkIndex >= chunks");
        }

        // Compute default chunk size (floored to previous integer)
        int chunkSize = computeDefaultChunkSize(size, chunks);

        // Compute the size of the last chunk
        int lastChunkSize = chunkSize + (size % chunks);

        // Return appropriate value
        return chunkIndex == chunks - 1 ? lastChunkSize : chunkSize;
    }

    public static BlockHeader createFrom(ByteBuffer payload,
                                         int sequence,
                                         int priority,
                                         Duration duration,
                                         int chunkCount) throws NoSuchAlgorithmException {
        Objects.requireNonNull(payload);
        Objects.requireNonNull(duration);

        // Create a split version of the block
        ByteBuffer dup = payload.duplicate();
        int size = dup.remaining();

        // Compute hash for payload
        Hash hash = Hash.computeSHA1(dup.duplicate());

        // Compute all chunk hashes
        List<Hash> chunkHashes = new ArrayList<>(chunkCount);
        for (int i = 0; i < chunkCount; i++) {
            // Compute limit for chunk and return computed chunk
            dup.limit(dup.position() + BlockHeader.computeChunkSize(size, chunkCount, i));
            chunkHashes.add(Hash.computeSHA1(dup));
        }

        // Create the block header for the push event
        return new BlockHeader(hash,
                               chunkHashes,
                               sequence,
                               priority,
                               size,
                               duration);
    }

    private final Hash hash;
    private final List<Hash> chunkHashes;
    private final int sequence;
    private final int priority;
    private final int size;
    private final Duration duration;

    public BlockHeader(Hash hash, List<Hash> chunkHashes, int sequence, int priority, int size, Duration duration) {
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

    public Hash getHash() {
        return hash;
    }

    public List<Hash> getChunkHashes() {
        return chunkHashes;
    }

    public int getChunkCount() {
        return chunkHashes.size();
    }

    public int getSequence() {
        return sequence;
    }

    public int getPriority() {
        return priority;
    }

    public int getSize() {
        return size;
    }

    public Duration getDuration() {
        return duration;
    }

    public int getDefaultChunkSize() {
        return computeDefaultChunkSize(size, chunkHashes.size());
    }

    public int getChunkSize(int chunkIndex) {
        return computeChunkSize(size, chunkHashes.size(), chunkIndex);
    }

    public ChunkHeader getChunk(int chunkIndex) {
        return new ChunkHeader(sequence, chunkIndex, getChunkSize(chunkIndex));
    }

    public Stream<ChunkHeader> getChunks() {
        return IntStream.range(0, getChunkCount()).mapToObj(this::getChunk);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BlockHeader that = (BlockHeader) o;

        if (sequence != that.sequence) return false;
        if (priority != that.priority) return false;
        if (size != that.size) return false;
        if (!hash.equals(that.hash)) return false;
        if (!chunkHashes.equals(that.chunkHashes)) return false;
        return duration.equals(that.duration);

    }

    @Override
    public int hashCode() {
        int result = hash.hashCode();
        result = 31 * result + chunkHashes.hashCode();
        result = 31 * result + sequence;
        result = 31 * result + priority;
        result = 31 * result + size;
        result = 31 * result + duration.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "BlockHeader{" +
               "hash=" + hash +
               ", chunkCount=" + getChunkCount() +
               ", sequence=" + sequence +
               ", priority=" + priority +
               ", size=" + size +
               ", defaultChunkSize=" + getDefaultChunkSize() +
               ", lastChunkSize=" + getChunkSize(getChunkCount() - 1) +
               ", duration=" + duration +
               '}';
    }
}