package org.feiyu.netty;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.concurrent.GlobalEventExecutor;

public final class MessageBroker {

    private static final ChannelGroup SERVER_VIEW_CHANNELS = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private static final ChannelGroup CLIENT_VIEW_CHANNELS = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private static final ChannelGroup SOCKET_CLIENTS = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    private MessageBroker() {
    }

    public static void register(Channel channel, String view) {
        if ("client".equals(view)) {
            CLIENT_VIEW_CHANNELS.add(channel);
        } else {
            SERVER_VIEW_CHANNELS.add(channel);
        }
    }

    public static void registerSocketClient(Channel channel) {
        SOCKET_CLIENTS.add(channel);
    }

    public static void onServerPageMessage(String text) {
        String message = "[服务器端] " + text;
        SERVER_VIEW_CHANNELS.writeAndFlush(new TextWebSocketFrame(message));
        CLIENT_VIEW_CHANNELS.writeAndFlush(new TextWebSocketFrame(message));
        broadcastToSocketClients("服务器端: " + text);
    }

    public static void onClientPageMessage(String text) {
        String message = "[客户端] " + text;
        SERVER_VIEW_CHANNELS.writeAndFlush(new TextWebSocketFrame(message));
        CLIENT_VIEW_CHANNELS.writeAndFlush(new TextWebSocketFrame(message));
        broadcastToSocketClients("客户端: " + text);
    }

    public static void onSocketClientMessage(Channel channel, String text) {
        String message = "[客户端] " + text;
        SERVER_VIEW_CHANNELS.writeAndFlush(new TextWebSocketFrame(message));
        CLIENT_VIEW_CHANNELS.writeAndFlush(new TextWebSocketFrame(message));
    }

    private static void broadcastToSocketClients(String message) {
        SOCKET_CLIENTS.writeAndFlush(message + "\n");
    }
}