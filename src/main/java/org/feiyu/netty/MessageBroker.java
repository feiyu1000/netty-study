package org.feiyu.netty;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.concurrent.GlobalEventExecutor;

public final class MessageBroker {

    private static final ChannelGroup SERVER_VIEW_CHANNELS = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private static final ChannelGroup CLIENT_VIEW_CHANNELS = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    private MessageBroker() {
    }

    public static void register(Channel channel, String view) {
        if ("client".equals(view)) {
            CLIENT_VIEW_CHANNELS.add(channel);
        } else {
            SERVER_VIEW_CHANNELS.add(channel);
        }
    }

    public static void broadcastServerView(String message) {
        SERVER_VIEW_CHANNELS.writeAndFlush(new TextWebSocketFrame(message));
    }

    public static void broadcastClientView(String message) {
        CLIENT_VIEW_CHANNELS.writeAndFlush(new TextWebSocketFrame("客户端发送：" + message));
    }
}