package de.probst.chunkedswarm.net.netty.handler.exception;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public class ExceptionHandler extends ChannelHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(ExceptionHandler.class);

    private final String name;

    public ExceptionHandler(String name) {
        Objects.requireNonNull(name);
        this.name = name;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.warn("Closing " + name + " [Channel: " + ctx.channel() + "] due to exception", cause);
        ctx.close();
    }
}
