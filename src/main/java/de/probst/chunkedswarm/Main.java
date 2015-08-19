package de.probst.chunkedswarm;

import de.probst.chunkedswarm.net.netty.Distributor;
import de.probst.chunkedswarm.net.netty.Forwarder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;


/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 19.05.15
 */
public class Main {


    public static void main(String[] args) throws InterruptedException, IOException {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            Distributor distributor = new Distributor(eventLoopGroup, new InetSocketAddress(1337));

            List<Forwarder> forwarders = new ArrayList<>(4);
            for (int i = 0; i < 20; i++) {
                int k = i;
                Forwarder f = new Forwarder(eventLoopGroup,
                                            new InetSocketAddress(20000 + i),
                                            new InetSocketAddress("kr0e.no-ip.info", 1337));
                f.getInitFuture().addListener(fut -> {
                    if (!fut.isSuccess()) {
                        System.out.println("Peer " + k + " connection result: " + fut.cause());
                    }
                });
                forwarders.add(f);
            }

            Runnable kill10 = () -> {
                for (int i = 0; i < 2; i++) {
                    try {
                        forwarders.remove(0).close();
                        Thread.sleep(73);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            while (!forwarders.isEmpty()) {

                System.in.read();
                kill10.run();
            }



            try {
                distributor.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            for (Forwarder forwarder : forwarders) {
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
