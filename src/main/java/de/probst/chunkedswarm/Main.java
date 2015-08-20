package de.probst.chunkedswarm;

import de.probst.chunkedswarm.net.netty.Distributor;
import de.probst.chunkedswarm.net.netty.Forwarder;
import de.probst.chunkedswarm.util.Block;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;


/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 19.05.15
 */
public class Main {


    public static void main(String[] args) throws InterruptedException, IOException {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            Distributor distributor = new Distributor(eventLoopGroup, new InetSocketAddress(1337));

            Map<Integer, Forwarder> portsToForwarders = new HashMap<>();
            Runnable createForwarder = () -> {
                for (int i = 0; i < 30; i++) {
                    int k = i;
                    if (portsToForwarders.containsKey(k)) {
                        continue;
                    }
                    Forwarder f = new Forwarder(eventLoopGroup,
                                                new InetSocketAddress(20000 + i),
                                                new InetSocketAddress("kr0e.no-ip.info", 1337));
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
                for (int i = 0; i < 2; i++) {
                    try {
                        if (portsToForwarders.isEmpty()) {
                            return;
                        }
                        Map.Entry<Integer, Forwarder> entry = portsToForwarders.entrySet().iterator().next();
                        portsToForwarders.remove(entry.getKey());
                        entry.getValue().close();
                        Thread.sleep(250);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            // Create 30 for the beginning
            for (int i = 0; i < 30; i++) {
                createForwarder.run();
            }

            int seq = 0;
            while (true) {
                int c = System.in.read();

                if (c == 'q') {
                    break;
                } else if (c == 'k') {
                    kill.run();
                } else if (c == 'a') {
                    createForwarder.run();
                } else if (c == 'p') {
                    distributor.distribute(new Block("0x000" + seq, seq++, 16, 8092, Duration.ofSeconds(10)));
                }
            }


            while (!portsToForwarders.isEmpty()) {
                System.in.read();
                kill.run();
            }

            try {
                distributor.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            for (Forwarder forwarder : portsToForwarders.values()) {
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
