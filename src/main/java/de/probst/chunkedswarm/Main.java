package de.probst.chunkedswarm;

import de.probst.chunkedswarm.net.netty.app.NettyDistributor;
import de.probst.chunkedswarm.net.netty.app.NettyForwarder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;


/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 19.05.15
 */
public class Main {


    public static void main(String[] args) throws InterruptedException, IOException {
        EventLoopGroup bossEventLoopGroup = new NioEventLoopGroup(1);
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            NettyDistributor distributor = new NettyDistributor(bossEventLoopGroup,
                                                                eventLoopGroup,
                                                                new InetSocketAddress(1337));

            Map<Integer, NettyForwarder> portsToForwarders = new HashMap<>();
            Consumer<Boolean> createForwarder = local -> {
                for (int i = 0; i < 100; i++) {
                    int k = i;
                    if (portsToForwarders.containsKey(k)) {
                        continue;
                    }
                    NettyForwarder f = new NettyForwarder(eventLoopGroup,
                                                          new InetSocketAddress(20000 + i),
                                                          new InetSocketAddress(local ? "localhost" : "kr0e.no-ip.info",
                                                                                1337));
                    f.getInitFuture().addListener(fut -> {
                        if (!fut.isSuccess()) {
                            System.out.println("Peer " + k + " connection result: " + fut.cause());
                        }
                    });
                    portsToForwarders.put(i, f);
                    return;
                }
            };

            Runnable kill = () -> {
                for (int i = 0; i < 1; i++) {
                    try {
                        if (portsToForwarders.isEmpty()) {
                            return;
                        }
                        Map.Entry<Integer, NettyForwarder> entry = portsToForwarders.entrySet().iterator().next();
                        portsToForwarders.remove(entry.getKey());
                        entry.getValue().close();
                        Thread.sleep(100);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            // Create remotes for the beginning
            for (int i = 0; i < 2; i++) {
                createForwarder.accept(false);
            }

            // Create locals for the beginning
            for (int i = 0; i < 2; i++) {
                createForwarder.accept(true);
            }


            ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 200);
            while (buf.hasRemaining()) {
                buf.put((byte) (Math.random() * 256));
            }
            buf.flip();

            int seq = 0;
            while (true) {
                int c = System.in.read();

                if (c == 'q') {
                    break;
                } else if (c == 'k') {
                    kill.run();
                } else if (c == 'a') {
                    //createForwarder.run();
                } else if (c == 'p') {

                    distributor.distribute(buf.duplicate(), seq, 0, Duration.ofSeconds(10));
                }
            }


            System.out.println("Shutting down... Keep pressing enter!");
            while (!portsToForwarders.isEmpty()) {
                System.in.read();
                kill.run();
            }

            try {
                distributor.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            for (NettyForwarder forwarder : portsToForwarders.values()) {
                try {
                    forwarder.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } finally {
            eventLoopGroup.shutdownGracefully();
        }

//
//        List<SwarmId> swarmIds = new ArrayList<>();
//        for (int i = 0; i < 4; i++) {
//            swarmIds.add(new SwarmId(UUID.randomUUID().toString(), new InetSocketAddress("localhost", 18000 + i)));
//        }
//        byte[] array = IOUtil.serialize(swarmIds);
//
//
//
//        System.out.println(array.length);

    }
}
