package cn.liqing.bili.live.danmu;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class Auth {
    private static final Gson GSON = new Gson();
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(5);

    public final long uid;
    public final long roomid;
    @SuppressWarnings("unused")
    public final int protover = 2;
    public final String buvid;
    @SuppressWarnings("unused")
    public final String platform = "web";
    @SuppressWarnings("unused")
    public final int type = 2;
    public volatile String key;
    /**
     * getDanmuInfo 返回的弹幕服务器列表，序列化时忽略。
     */
    public transient volatile List<Host> hosts = List.of();

    private final String cookie;

    public Auth(long roomid, long uid, String buvid, String key) {
        this(roomid, uid, buvid, key, "", List.of());
    }

    private Auth(long roomid, long uid, String buvid, String key, String cookie, List<Host> hosts) {
        this.roomid = roomid;
        this.uid = uid;
        this.buvid = buvid;
        this.key = key;
        this.cookie = cookie;
        this.hosts = hosts;
    }

    public static @NotNull Auth create(long roomid, String cookie)
            throws IOException, InterruptedException {
        String buvid = extractCookieValue("buvid3", cookie);
        long uid = Long.parseLong(Objects.requireNonNull(
                extractCookieValue("DedeUserID", cookie)));
        DanmuInfo info = fetchDanmuInfo(roomid, cookie);
        return new Auth(roomid, uid, buvid, info.token, cookie, info.hosts);
    }

    public static @NotNull Auth create(long roomid)
            throws IOException, InterruptedException {
        DanmuInfo info = fetchDanmuInfo(roomid, "");
        return new Auth(roomid, 0, generateUUID(), info.token, "", info.hosts);
    }

    /**
     * 重新拉取 token 与服务器列表，用于断线重连。
     */
    public void refresh() throws IOException, InterruptedException {
        DanmuInfo info = fetchDanmuInfo(roomid, cookie);
        this.key = info.token;
        this.hosts = info.hosts;
    }

    private static DanmuInfo fetchDanmuInfo(long roomId, String cookie)
            throws IOException, InterruptedException {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(HTTP_TIMEOUT)
                .build();

        Map<String, Object> params = new HashMap<>();
        params.put("id", roomId);
        params.put("type", "0");

        String signedQuery;
        try {
            signedQuery = BiliWbiSign.wbiSign(params);
        } catch (Exception e) {
            throw new RuntimeException("签名失败", e);
        }

        String baseUrl = "https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo";
        String fullUrl = baseUrl + "?" + signedQuery;

        HttpRequest request = HttpRequest.newBuilder(URI.create(fullUrl))
                .header("Accept", "*/*")
                .header("Cookie", cookie)
                .timeout(HTTP_TIMEOUT)
                .build();

        HttpResponse<String> response = client.send(request,
                HttpResponse.BodyHandlers.ofString());

        JsonObject root = GSON.fromJson(response.body(), JsonObject.class);
        JsonElement data = root.get("data");
        if (data == null || !data.isJsonObject()) {
            throw new IOException("获取弹幕服务器信息失败: code="
                    + valueOf(root, "code") + ", message=" + valueOf(root, "message"));
        }

        JsonObject dataObj = data.getAsJsonObject();
        String token = dataObj.get("token").getAsString();

        List<Host> hosts = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        JsonElement hostList = dataObj.get("host_list");
        if (hostList != null && hostList.isJsonArray()) {
            JsonArray list = hostList.getAsJsonArray();
            for (JsonElement element : list) {
                if (!element.isJsonObject()) {
                    continue;
                }
                JsonObject hostObj = element.getAsJsonObject();
                JsonElement hostName = hostObj.get("host");
                if (hostName == null || hostName.isJsonNull()) {
                    continue;
                }
                String host = hostName.getAsString();
                if (!seen.add(host)) {
                    continue;
                }
                int wssPort = hostObj.has("wss_port") ? hostObj.get("wss_port").getAsInt() : 2245;
                hosts.add(new Host(host, wssPort));
            }
        }

        return new DanmuInfo(token, List.copyOf(hosts));
    }

    private static String valueOf(JsonObject root, String name) {
        JsonElement element = root.get(name);
        return element == null || element.isJsonNull() ? "" : element.getAsString();
    }

    private static @Nullable String extractCookieValue(String cookieName,
                                                       @NotNull String cookieString) {
        String[] cookies = cookieString.split(";");
        for (String cookie : cookies) {
            cookie = cookie.trim();
            if (cookie.startsWith(cookieName + "=")) {
                return cookie.substring(cookieName.length() + 1);
            }
        }
        return null;
    }

    private static @NotNull String generateUUID() {
        UUID uuid = UUID.randomUUID();
        long currentTimeMillis = System.currentTimeMillis();
        String timestampSuffix = String.valueOf(currentTimeMillis).substring(0, 5);
        return uuid.toString().replace("-", "") + timestampSuffix + "infoc";
    }

    private record DanmuInfo(String token, List<Host> hosts) {
    }

    public static final class Host {
        public final String host;
        public final int wssPort;

        public Host(String host, int wssPort) {
            this.host = host;
            this.wssPort = wssPort;
        }

        public String wssUrl() {
            return "wss://" + host + ":" + wssPort + "/sub";
        }
    }
}
