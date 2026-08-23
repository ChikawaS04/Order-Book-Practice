package net;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Real loopback proof: the server binds, a real Netty WS client handshakes, and the server
 *  registers then drops the client in its ChannelGroup. */
class WebSocketServerTest {

    private WebSocketServer server;

    @BeforeEach
    void startServer() throws InterruptedException {
        server = new WebSocketServer(0); // ephemeral port
        server.start();
    }

    @AfterEach
    void stopServer() throws InterruptedException {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void clientHandshakeJoinsAndLeavesChannelGroup() throws Exception {
        EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        CountDownLatch handshakeDone = new CountDownLatch(1);
        try {
            URI uri = URI.create("ws://127.0.0.1:" + server.boundPort() + "/ws");

            Bootstrap b = new Bootstrap();
            b.group(clientGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new HttpClientCodec());
                            ch.pipeline().addLast(new HttpObjectAggregator(64 * 1024));
                            ch.pipeline().addLast(new WebSocketClientProtocolHandler(
                                    WebSocketClientHandshakerFactory.newHandshaker(
                                            uri, WebSocketVersion.V13, null, false,
                                            new DefaultHttpHeaders())));
                            ch.pipeline().addLast(new SimpleChannelInboundHandler<WebSocketFrame>() {
                                @Override
                                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                                    if (evt == WebSocketClientProtocolHandler
                                            .ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                                        handshakeDone.countDown();
                                    }
                                }

                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
                                    // no server->client frames in P4-4
                                }
                            });
                        }
                    });

            Channel clientChannel = b.connect(uri.getHost(), server.boundPort()).sync().channel();

            assertTrue(handshakeDone.await(5, TimeUnit.SECONDS), "client handshake should complete");
            assertTrue(awaitGroupSize(1, 2000), "server should register the connected client");

            clientChannel.close().sync();
            assertTrue(awaitGroupSize(0, 2000), "server should drop the client on disconnect");
        } finally {
            clientGroup.shutdownGracefully();
        }
    }

    /** Group membership updates on the server's event loop, so poll with a bounded deadline. */
    private boolean awaitGroupSize(int expected, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (server.getChannelGroup().size() == expected) {
                return true;
            }
            Thread.sleep(10);
        }
        return server.getChannelGroup().size() == expected;
    }
}