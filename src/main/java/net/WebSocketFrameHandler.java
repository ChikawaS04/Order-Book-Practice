package net;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-connection tail handler for the WebSocket pipeline. P4-4 scaffold: on a completed
 * handshake it registers the channel in the shared {@link ChannelGroup}; it does not yet
 * handle inbound frames. P4-5 fills {@link #channelRead0} with the
 * TextWebSocketFrame → Jackson → JsonToFix → OrderGateway.onFrame path (on the single worker
 * thread, preserving the single inbound producer).
 *
 * <p>Registration happens on {@link WebSocketServerProtocolHandler.HandshakeComplete}, not on
 * channelActive: a channel is added only once it is actually speaking WebSocket, so the P4-6
 * publisher never writes a WS frame to a still-upgrading HTTP channel. {@code DefaultChannelGroup}
 * auto-removes a channel when it closes, so there is no explicit deregistration to maintain.
 *
 * <p>A fresh instance is created per channel by the server's initializer, so it is intentionally
 * not {@code @Sharable}.
 */
public class WebSocketFrameHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

    private static final Logger log = LoggerFactory.getLogger(WebSocketFrameHandler.class);

    private final ChannelGroup channelGroup;

    public WebSocketFrameHandler(ChannelGroup channelGroup) {
        this.channelGroup = channelGroup;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
            channelGroup.add(ctx.channel());
            log.info("WS client connected: {} ({} total)",
                    ctx.channel().remoteAddress(), channelGroup.size());
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
        // P4-4 stub: inbound frames are ignored. P4-5 deserializes here.
    }
}