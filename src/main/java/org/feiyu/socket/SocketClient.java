package org.feiyu.socket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class SocketClient {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 8888;

    public static void main(String[] args) throws IOException {
        try (Socket socket = new Socket(HOST, PORT);
             BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
             PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {
            System.out.println("已连接服务器：" + socket.getRemoteSocketAddress() + "，输入内容发送，输入 exit 退出");
            String line;
            while ((line = console.readLine()) != null) {
                if ("exit".equalsIgnoreCase(line)) {
                    break;
                }
                writer.println(line);
            }
        }
    }
}