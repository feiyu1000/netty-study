package org.feiyu.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public final class NettyHttpServer {

    public static final int HTTP_PORT = 8080;
    private static final String WS_PATH = "/ws";
    private static final AttributeKey<String> VIEW_KEY = AttributeKey.valueOf("wsView");

    private NettyHttpServer() {
    }

    public static void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new HttpServerCodec())
                                    .addLast(new HttpObjectAggregator(65536))
                                    .addLast(new HttpStaticPageHandler())
                                    .addLast(new WebSocketServerProtocolHandler(WS_PATH, true))
                                    .addLast(new WebSocketFrameHandler());
                        }
                    });
            ChannelFuture future = bootstrap.bind(HTTP_PORT).sync();
            System.out.println("前端页面服务已启动：http://localhost:" + HTTP_PORT + "/server（服务器端消息） 和 /client（客户端消息）");
            future.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    static class HttpStaticPageHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
            if (isWebSocketUpgrade(request) && request.uri().startsWith(WS_PATH)) {
                QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
                String view = "server";
                java.util.List<String> params = decoder.parameters().get("channel");
                if (params != null && !params.isEmpty()) {
                    view = params.get(0);
                }
                ctx.channel().attr(VIEW_KEY).set(view);
                ctx.fireChannelRead(request.retain());
                return;
            }

            String page = loadPage(request.uri());
            if (page == null) {
                sendResponse(ctx, HttpResponseStatus.NOT_FOUND, "404 Not Found");
            } else {
                sendResponse(ctx, HttpResponseStatus.OK, page);
            }
        }

        private static boolean isWebSocketUpgrade(FullHttpRequest request) {
            return request.headers().contains(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET, true)
                    && request.headers().contains(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE, true);
        }

        private static String loadPage(String uri) {
            String path = "/".equals(uri) ? "/server" : uri;
            String resource;
            switch (path) {
                case "/server":
                    resource = "static/server.html";
                    break;
                case "/client":
                    resource = "static/client.html";
                    break;
                default:
                    return null;
            }
            try (InputStream in = HttpStaticPageHandler.class.getClassLoader().getResourceAsStream(resource)) {
                if (in == null) {
                    return null;
                }
                StringBuilder sb = new StringBuilder();
                try (java.io.BufferedReader reader = new java.io.BufferedReader(
                        new java.io.InputStreamReader(in, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        sb.append(line).append('\n');
                    }
                }
                return sb.toString();
            } catch (Exception e) {
                return null;
            }
        }

        private static void sendResponse(ChannelHandlerContext ctx, HttpResponseStatus status, String content) {
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, status, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }

    static class WebSocketFrameHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
                String view = ctx.channel().attr(VIEW_KEY).get();
                MessageBroker.register(ctx.channel(), view == null ? "server" : view);
                ctx.channel().writeAndFlush(new TextWebSocketFrame("已连接消息推送服务"));
            }
            super.userEventTriggered(ctx, evt);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
            if (frame instanceof TextWebSocketFrame) {
                String text = ((TextWebSocketFrame) frame).text();
                String view = ctx.channel().attr(VIEW_KEY).get();
                if ("client".equals(view)) {
                    MessageBroker.onClientPageMessage(text);
                } else {
                    MessageBroker.onServerPageMessage(text);
                }
            } else if (frame instanceof CloseWebSocketFrame) {
                ctx.close();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
}