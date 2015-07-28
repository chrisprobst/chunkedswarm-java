package de.probst.chunkedswarm.util;

import java.util.Collection;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 28.07.15
 */
public final class Util {

    private Util() {

    }

    public static void closeAllAndThrow(Collection<? extends AutoCloseable> closeables) throws Exception {
        Exception any = closeAllAndGetException(closeables, null);
        if (any != null) {
            throw any;
        }
    }

    public static Exception closeAllAndGetException(Collection<? extends AutoCloseable> closeables,
                                                    Exception suppressed) {
        Objects.requireNonNull(closeables);
        Exception any = suppressed;
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception e) {
                if (any == null) {
                    any = e;
                } else {
                    e.addSuppressed(any);
                    any = e;
                }
            }
        }
        return any;
    }
}
