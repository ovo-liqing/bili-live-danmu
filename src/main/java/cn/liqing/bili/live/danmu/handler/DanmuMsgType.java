package cn.liqing.bili.live.danmu.handler;

import cn.liqing.bili.live.danmu.Message;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

/**
 * 读取 DANMU_MSG 的弹幕类型（info[0][12]），对协议字段缺失返回 null。
 * cmd 可能是 "DANMU_MSG" 或 "DANMU_MSG:4:0:2:2:2:2" 这类带后缀的形式。
 */
final class DanmuMsgType {
    static final int EMOJI = 1;

    private DanmuMsgType() {
    }

    static Integer resolve(Message message) {
        if (message.cmd == null || !message.cmd.startsWith("DANMU_MSG")) {
            return null;
        }
        JsonElement info = message.info;
        if (info == null || !info.isJsonArray()) {
            return null;
        }
        JsonArray infoArray = info.getAsJsonArray();
        if (infoArray.size() == 0 || !infoArray.get(0).isJsonArray()) {
            return null;
        }
        JsonArray meta = infoArray.get(0).getAsJsonArray();
        if (meta.size() <= 12) {
            return null;
        }
        JsonElement type = meta.get(12);
        if (!type.isJsonPrimitive() || !type.getAsJsonPrimitive().isNumber()) {
            return null;
        }
        return type.getAsInt();
    }
}
