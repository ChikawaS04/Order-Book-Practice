package net;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Netty WebSocket server (SRS §3.6). P4-4 scaffold: stands up the reactor, exposes {@code /ws},
 * and registers each fully-handshaken client in a shared {@link ChannelGroup}. No business
 * logic yet — P4-5 wires the inbound frame path into the FIX gateway, P4-6 fans execution/
 * book frames out over the same group.
 *
 * <p><b>Single-writer invariant (guide decision 2).</b> The worker group is <b>one thread</b>,
 * so every inbound publish happens on a single thread and the inbound Disruptor stays
 * {@code ProducerType.SINGLE} (§5.2). Do not widen the worker group without revisiting that.
 *
 * <p>{@code ws://}, no TLS (demo, §3.6).
 */
public final class WebSocketServer {

    private static final Logger log = LoggerFactory.getLogger(WebSocketServer.class);

    private static final String WEBSOCKET_PATH = "/ws";
    private static final int MAX_HTTP_CONTENT_LENGTH = 64 * 1024;

    private final int port;

    /** Connected, fully-handshaken clients. Shared with the publisher (P4-6). Auto-evicts on close. */
    private final ChannelGroup channelGroup =
            new DefaultChannelGroup("ws-clients", GlobalEventExecutor.INSTANCE);

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public WebSocketServer(int port) {
        this.port = port;
    }

    /** Shared connected-client registry; P4-6's publisher writes execution/book frames to this. */
    public ChannelGroup getChannelGroup() {
        return channelGroup;
    }

    /** The actually-bound port. Useful when constructed with port 0 (ephemeral) in tests. */
    public int boundPort() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    /** Bind and begin accepting. Blocks until the listen socket is bound. */
    public void start() throws InterruptedException {
        // 4.2 API: NioEventLoopGroup is deprecated; use MultiThreadIoEventLoopGroup + NioIoHandler.
        bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory()); // single worker

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new HttpServerCodec());
                        p.addLast(new HttpObjectAggregator(MAX_HTTP_CONTENT_LENGTH));
                        p.addLast(new WebSocketServerProtocolHandler(WEBSOCKET_PATH));
                        p.addLast(new WebSocketFrameHandler(channelGroup));
                    }
                });

        serverChannel = b.bind(port).sync().channel();
        log.info("WebSocket server listening on ws://0.0.0.0:{}{}", boundPort(), WEBSOCKET_PATH);
    }

    /** Stop accepting, close every client channel, release the loops. Reverse order of start(). */
    public void stop() throws InterruptedException {
        if (serverChannel != null) {
            serverChannel.close().sync();
        }
        channelGroup.close().awaitUninterruptibly();
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        log.info("WebSocket server stopped");
    }
}