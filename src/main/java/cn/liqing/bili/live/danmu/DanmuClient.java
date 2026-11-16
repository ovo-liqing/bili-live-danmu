package cn.liqing.bili.live.danmu;

import com.google.gson.Gson;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class DanmuClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(DanmuClient.class);
    private static final Gson GSON = new Gson();
    private static final URI DEFAULT_SERVER_URI = URI.create("wss://broadcastlv.chat.bilibili.com:2245/sub");
    private static final long MAX_RECONNECT_DELAY_SECONDS = 30;

    private final URI serverUri;
    private volatile WebSocketClient wsClient;
    private ConnectionListener connectionListener;
    private final List<MessageHandler> handlers = new ArrayList<>();
    private ScheduledExecutorService heartbeatScheduler;
    private ScheduledFuture<?> heartbeatTask;

    private Auth auth;
    private volatile boolean autoReconnect = true;
    private volatile boolean closedByUser;
    private volatile boolean reconnectScheduled;
    private int reconnectAttempts;
    private ScheduledExecutorService reconnectScheduler;

    public DanmuClient() {
        this(null);
    }

    /**
     * @param serverUri 固定连接地址；传 null 时使用 getDanmuInfo 返回的服务器列表
     */
    public DanmuClient(URI serverUri) {
        this.serverUri = serverUri;
    }

    public void connect(Auth auth) {
        this.auth = auth;
        closedByUser = false;
        reconnectAttempts = 0;
        reconnectScheduled = false;
        stopHeartbeat();

        WebSocketClient previous = wsClient;
        wsClient = null;
        if (previous != null) {
            previous.close();
        }

        openSocket();
    }

    private void openSocket() {
        URI uri = resolveServerUri();
        WebSocketClient client = new WebSocketClient(uri) {
            @Override
            public void onOpen(ServerHandshake handshakedata) {
                if (wsClient != this) {
                    return;
                }
                reconnectAttempts = 0;
                if (connectionListener != null) {
                    connectionListener.onOpen();
                }

                try {
                    Auth current = auth;
                    byte[] body = GSON.toJson(current).getBytes(StandardCharsets.UTF_8);
                    send(new Packet(Packet.Operation.AUTH, body).pack());
                } catch (Exception e) {
                    throw new RuntimeException("认证出错", e);
                }

                startHeartbeat();
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                if (wsClient != this) {
                    return;
                }
                if (connectionListener != null) {
                    connectionListener.onClose(code, reason, remote);
                }
                stopHeartbeat();

                if (closedByUser) {
                    wsClient = null;
                    return;
                }
                scheduleReconnect();
            }

            @Override
            public void onError(Exception ex) {
                if (wsClient != this) {
                    return;
                }
                if (connectionListener != null) {
                    connectionListener.onError(ex);
                }
                if (!closedByUser && !isOpen()) {
                    scheduleReconnect();
                }
            }

            @Override
            public void onMessage(String message) {
            }

            @Override
            public void onMessage(ByteBuffer bytes) {
                List<Packet> packets = Packet.unPack(bytes);
                packets.forEach(DanmuClient.this::onPacket);
            }
        };
        wsClient = client;
        client.connect();
    }

    private URI resolveServerUri() {
        if (serverUri != null) {
            return serverUri;
        }
        Auth current = auth;
        if (current == null || current.hosts.isEmpty()) {
            return DEFAULT_SERVER_URI;
        }
        List<Auth.Host> hosts = current.hosts;
        Auth.Host host = hosts.get(ThreadLocalRandom.current().nextInt(hosts.size()));
        return URI.create(host.wssUrl());
    }

    private void scheduleReconnect() {
        if (closedByUser || !autoReconnect || reconnectScheduled) {
            return;
        }
        reconnectScheduled = true;
        int attempt = reconnectAttempts++;
        long delay = Math.min(MAX_RECONNECT_DELAY_SECONDS, 1L << Math.min(attempt, 5));
        LOGGER.info("弹幕连接已断开，{} 秒后进行第 {} 次重连", delay, attempt + 1);

        reconnectScheduler().schedule(() -> {
            reconnectScheduled = false;
            if (closedByUser || !autoReconnect) {
                return;
            }
            WebSocketClient current = wsClient;
            if (current != null && current.isOpen()) {
                return;
            }
            Auth currentAuth = auth;
            if (currentAuth != null) {
                try {
                    currentAuth.refresh();
                } catch (Exception ex) {
                    LOGGER.warn("刷新弹幕服务器信息失败，使用原 token 重连", ex);
                }
            }
            if (closedByUser) {
                return;
            }
            openSocket();
        }, delay, TimeUnit.SECONDS);
    }

    private synchronized ScheduledExecutorService reconnectScheduler() {
        if (reconnectScheduler == null || reconnectScheduler.isShutdown()) {
            reconnectScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "BiliDanmu-Reconnect");
                thread.setDaemon(true);
                return thread;
            });
        }
        return reconnectScheduler;
    }

    private void startHeartbeat() {
        heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "BiliDanmu-Heartbeat");
            thread.setDaemon(true);
            return thread;
        });

        heartbeatTask = heartbeatScheduler.scheduleAtFixedRate(() -> {
            WebSocketClient current = wsClient;
            if (current != null && current.isOpen()) {
                current.send(new Packet(Packet.Operation.HEARTBEAT, new byte[0]).pack());
            }
        }, 0, 30, TimeUnit.SECONDS);
    }

    private void stopHeartbeat() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
        }
        if (heartbeatScheduler != null) {
            heartbeatScheduler.shutdown();
            heartbeatScheduler = null;
        }
    }

    public void disconnect() {
        closedByUser = true;
        reconnectScheduled = false;
        stopHeartbeat();

        ScheduledExecutorService scheduler = reconnectScheduler;
        reconnectScheduler = null;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }

        WebSocketClient current = wsClient;
        if (current != null) {
            current.close();
        }
    }

    public boolean isOpen() {
        WebSocketClient current = wsClient;
        return current != null && current.isOpen();
    }

    /**
     * 断线后是否自动重连，默认开启。
     */
    public void setAutoReconnect(boolean autoReconnect) {
        this.autoReconnect = autoReconnect;
    }

    public boolean isAutoReconnect() {
        return autoReconnect;
    }

    public void setListener(ConnectionListener listener) {
        this.connectionListener = listener;
    }

    public void addHandler(MessageHandler handler) {
        handlers.add(handler);
    }

    public void removeHandler(MessageHandler handler) {
        handlers.remove(handler);
    }

    private void onPacket(@NotNull Packet packet) {
        if (packet.operation == Packet.Operation.SEND_SMS_REPLY) {
            String bodyStr = new String(packet.body, StandardCharsets.UTF_8);
            LOGGER.debug(bodyStr);

            Message message;
            try {
                message = GSON.fromJson(bodyStr, Message.class);
            } catch (Exception ex) {
                throw new RuntimeException("解析消息出错", ex);
            }

            if (message.cmd == null) {
                throw new RuntimeException("消息包中没有cmd");
            }

            for (MessageHandler handler : handlers) {
                if (handler.canHandle(message)) {
                    handler.handle(message);
                }
            }
        }
    }

    public static @NotNull String generateRandomNumber() {
        return String.valueOf(new Random().nextInt(Integer.MAX_VALUE));
    }

    public static void send(String cookie, String roomId, String message) throws IOException {
        String url = "https://api.live.bilibili.com/msg/send";
        String csrf = extractCookieValue("bili_jct", cookie);
        String postData = String.format(
                "color=16777215&fontsize=25&mode=1&msg=%s&rnd=%d&roomid=%s&bubble=0&csrf_token=%s&csrf=%s",
                java.net.URLEncoder.encode(message, StandardCharsets.UTF_8),
                System.currentTimeMillis() / 1000,
                roomId,
                csrf,
                csrf
        );

        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        connection.setRequestProperty("Cookie", cookie);

        try (OutputStream os = connection.getOutputStream()) {
            os.write(postData.getBytes(StandardCharsets.UTF_8));
        }

        int responseCode = connection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new IOException("发送弹幕失败，HTTP 状态码: " + responseCode);
        }
    }

    private static String extractCookieValue(String cookieName, String cookieString) {
        String[] cookies = cookieString.split(";");
        for (String cookie : cookies) {
            cookie = cookie.trim();
            if (cookie.startsWith(cookieName + "=")) {
                return cookie.substring(cookieName.length() + 1);
            }
        }
        return null;
    }
}
