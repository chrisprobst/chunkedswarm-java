package de.probst.chunkedswarm.util;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public final class SwarmIdSet implements Serializable {

    private final Set<SwarmId> swarmIds;

    public SwarmIdSet(Set<SwarmId> swarmIds) {
        Objects.requireNonNull(swarmIds);
        this.swarmIds = swarmIds;
    }

    public SwarmIdSet() {
        this(new HashSet<>());
    }

    public SwarmIdSet(Collection<SwarmId> swarmIds) {
        this(new HashSet<>(swarmIds));
    }

    public Set<SwarmId> get() {
        return swarmIds;
    }

    public SwarmIdSet computeMissing(SwarmIdSet other) {
        SwarmIdSet missing = new SwarmIdSet();
        other.get().stream().filter(SwarmId -> !swarmIds.contains(SwarmId)).forEach(missing.get()::add);
        return missing;
    }

    public SwarmIdSet computeAdditional(SwarmIdSet other) {
        SwarmIdSet additional = new SwarmIdSet();
        swarmIds.stream().filter(SwarmId -> !other.get().contains(SwarmId)).forEach(additional.get()::add);
        return additional;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SwarmIdSet that = (SwarmIdSet) o;

        return swarmIds.equals(that.swarmIds);

    }

    @Override
    public int hashCode() {
        return swarmIds.hashCode();
    }

    @Override
    public String toString() {
        return "SwarmIdSet{" +
               "swarmIds=" + swarmIds +
               '}';
    }
}