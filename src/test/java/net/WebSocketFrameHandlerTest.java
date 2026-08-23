package net;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Unit-level contract for the handler's group membership, no sockets involved. */
class WebSocketFrameHandlerTest {

    @Test
    void addsChannelToGroupOnHandshakeComplete() throws Exception {
        ChannelGroup group = new DefaultChannelGroup("test", GlobalEventExecutor.INSTANCE);
        EmbeddedChannel ch = new EmbeddedChannel(new WebSocketFrameHandler(group));

        assertTrue(group.isEmpty(), "no members before the handshake completes");

        ch.pipeline().fireUserEventTriggered(new WebSocketServerProtocolHandler.HandshakeComplete(
                "/ws", new DefaultHttpHeaders(), null));

        assertEquals(1, group.size());
        assertTrue(group.contains(ch));

        ch.close().sync();
        assertTrue(group.isEmpty(), "DefaultChannelGroup auto-removes the closed channel");
    }

    @Test
    void ignoresUnrelatedUserEvents() throws Exception {
        ChannelGroup group = new DefaultChannelGroup("test", GlobalEventExecutor.INSTANCE);
        EmbeddedChannel ch = new EmbeddedChannel(new WebSocketFrameHandler(group));

        ch.pipeline().fireUserEventTriggered(new Object());

        assertTrue(group.isEmpty(), "a non-handshake user event must not register the channel");
        ch.close().sync();
    }
}