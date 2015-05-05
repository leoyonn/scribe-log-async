/**
 *
 * Log.java
 * @date 14-11-3 下午5:45
 * @author leo [leoyonn@gmail.com]
 * [CopyRight] All Rights Reserved.
 */

package me.lyso.log.scribe;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.commons.lang3.StringUtils;

/**
 * Build a scribe log message as whole string split with ','.
 * Usage:
 * <CODE>
 * new Log().$1_host(host).$2_action(UsageAction.msg_sent).$3_from(from).$5_msgId(msgId)....done(slogger);
 * </CODE>
 * NOTICE: You can skip field, but can't set field out-of-order or in descending order.
 *
 * @author leo
 */
public class Log {
    protected static final FastDateFormat TimeFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    /** "," shouldn't appeared in log fields, replace it to this value.*/
    protected static final char CommaReplace = '`';
    /** For a filed, its size couldn't exceed this value; else cut it to MaxFieldLen - 2, and add tail dots "..". */
    protected static final int MaxFieldLen = 64;
    protected static final int LastIndex = 0x10;

    protected final StringBuilder builder = new StringBuilder(512);
    protected int index = 0;

    public Log() {
    }

    /**
     * Verify that my index is less than current setting field index.
     * If my index was lagged behind more than one (such as my index is 1 and setting field 4),
     * just catch up with ',''s.
     *
     * @param index
     */
    protected Log index(int index) {
        if (this.index >= index) {
            throw new IllegalStateException("Field index should only increase.");
        }
        return catchUp(index);
    }

    protected Log catchUp(int index) {
        while (++this.index < index) {
            builder.append(',');
        }
        return this;
    }

    protected Log append(String v) {
        if (StringUtils.isNotBlank(v)) {
            boolean exceeded = false;
            int len = v.length();
            if (len > MaxFieldLen) {
                exceeded = true;
                len = MaxFieldLen - 2;
            }
            for (int i = 0; i < len; ++i) {
                char c = v.charAt(i);
                builder.append(c == ',' ? CommaReplace : c);
            }
            if (exceeded) {
                builder.append("..");
            }
        }
        builder.append(',');
        return this;
    }

    protected Log append(int v) {
        builder.append(v).append(',');
        return this;
    }

    public String done() {
        return catchUp(LastIndex).builder.toString();
    }

    public boolean done(ScribeLogger logger) {
        return logger.log(done());
    }

    public static class Usage extends Log {
        public Usage() {
            super();
        }

        public Usage $1_host(String host) {
            return (Usage) index(1).append(host);
        }

        public Usage $2_action(UsageAction action) {
            return (Usage) index(2).append(action.name());
        }

        public Usage $3_from(String from) {
            return (Usage) index(3).append(from);
        }

        public Usage $4_to(String to) {
            return (Usage) index(4).append(to);
        }

        public Usage $5_msgId(String msgId) {
            return (Usage) index(5).append(msgId);
        }

        public Usage $6_msgType(String msgType) {
            return (Usage) index(6).append(msgType);
        }

        public Usage $7_fromIp(String fromIp) {
            return (Usage) index(7).append(fromIp);
        }

        public Usage $8_timestamp(long now) {
            return (Usage) index(8).append(TimeFormat.format(now));
        }

        public Usage $8_now() {
            return $8_timestamp(System.currentTimeMillis());
        }

        public Usage $9_chid(int chid) {
            return (Usage) index(9).append(chid);
        }

        public Usage $A_clientIp(String ip) {
            return (Usage) index(0xA).append(ip);
        }

        public Usage $B_feIp(String ip) {
            return (Usage) index(0xB).append(ip);
        }

        public Usage $C_appId(String appId) {
            return (Usage) index(0xC).append(appId);
        }

        public Usage $D_packageName(String packageName) {
            return (Usage) index(0xD).append(packageName);
        }

        public Usage $E_os(String os) {
            return (Usage) index(0xE).append(os);
        }

        public Usage $F_model(String model) {
            return (Usage) index(0xF).append(model);
        }

        public Usage $G_sdkVersion(String sdkVersion) {
            return (Usage) index(0x10).append(sdkVersion);
        }

    }

    public static class Error extends Log {
        public Error() {
            super();
        }
    }

}
