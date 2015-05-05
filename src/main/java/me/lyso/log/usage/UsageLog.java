/**
 * UsageLog.java
 * [CopyRight]
 * @author leo [leoyonn@gmail.com]
 * @date Sep 3, 2013 3:20:42 PM
 */
package me.lyso.log.usage;

import java.text.SimpleDateFormat;

import org.apache.commons.lang3.StringUtils;

import me.lyso.log.scribe.ScribeLogger;

/**
 * @author leo
 */
public class UsageLog {
    private static final String SERVER = "lyso.me";
    private final SimpleDateFormat TIME_FMT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    // "," shouldn't appeared in log fields, replace it to this value.
    private static final char REPLACE_COMMA_TO = '`';
    // For a filed, its size couldn't exceed this value.
    // Else cut it to MAX_FIELD_SIZE - 2, and add tail dots "..".
    private static final int MAX_FIELD_SIZE = 64;

    /** module == msgType */
    private String module = "m:unset";
    private long chid;
    private MessageDirection dir;
    private String action;
    private String uuid;
    private String resource;
    private String msgId;
    private String appId;
    private String packageName;
    // Used in send message/ack message
    private String topicOrAlias;
    // Message create timestamp.
    private Long messageCreateTimestamp;

    private String fromIp = "";
    private String clientIp = "";
    private String frontendIp = "";
    private String os = "";
    private String model = "";
    private String sdkVersion = "";

    public int getCount() {
        return count;
    }

    public UsageLog setCount(int count) {
        this.count = count;
        return this;
    }

    /**
     * Repeated count of the message, usefull for broadcast messages, or batch messages.
     */
    private int count = 1;

    private StringBuilder sb = new StringBuilder();
    boolean built = false;
    private String msg = "";

    public UsageLog() {}

    public String getModule() {
        return module;
    }

    public UsageLog setModule(String module) {
        this.module = module;
        return this;
    }

    public UsageLog setChid(long chid) {
        this.chid = chid;
        return this;
    }

    public long getChid() {
        return chid;
    }

    public MessageDirection getDir() {
        return dir;
    }

    public UsageLog setDir(MessageDirection dir) {
        this.dir = dir;
        this.built = false;
        return this;
    }

    public String getAction() {
        return action;
    }

    public UsageLog setAction(String action) {
        this.action = action;
        this.built = false;
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public UsageLog setUuid(String uuid) {
        this.uuid = uuid;
        this.built = false;
        return this;
    }

    public String getResource() {
        return resource;
    }

    public UsageLog setResource(String resource) {
        this.resource = resource;
        this.built = false;
        return this;
    }

    public String getMsgId() {
        return msgId;
    }

    public UsageLog setMsgId(String msgId) {
        this.msgId = msgId;
        this.built = false;
        return this;
    }

    public String getAppId() {
        return appId;
    }

    public UsageLog setAppId(String appId) {
        this.appId = appId;
        this.built = false;
        return this;
    }

    public String getPackageName() {
        return packageName;
    }

    public UsageLog setPackageName(String packageName) {
        this.packageName = packageName;
        this.built = false;
        return this;
    }

    public UsageLog setTopicOrAlias(String value) {
        this.topicOrAlias = value;
        return this;
    }

    public String getTopicOrAlias() {
        return topicOrAlias;
    }

    public UsageLog setMessageCreateTimestamp(Long timestamp) {
        this.messageCreateTimestamp = timestamp;
        return this;
    }

    public Long getMessageCreateTimestamp() {
        return messageCreateTimestamp;
    }

    public String getOs() {
        return os;
    }
    public UsageLog setOs(String os) {
        this.os = os;
        return this;
    }

    public String getSdkVersion() {
        return sdkVersion;
    }
    public UsageLog setSdkVersion(String version) {
        this.sdkVersion = version;
        return this;
    }

    public String getClientIp() {
        return clientIp;
    }

    public UsageLog setClientIp(String clientIp) {
        this.clientIp = clientIp;
        return this;
    }

    public String getFrontendIp() {
        return frontendIp;
    }
    public UsageLog setFrontendIp(String frontendIp) {
        this.frontendIp = frontendIp;
        return this;
    }

    public String getFromIp() {
        return fromIp;
    }
    public UsageLog setFromIp(String fromIp) {
        this.fromIp = fromIp;
        return this;
    }

    public String getModel() {
        return model;
    }
    public UsageLog setModel(String model) {
        this.model = model;
        return this;
    }

    public String build() {
        if (built) {
            return msg;
        }
        sb.setLength(0);
        long now = System.currentTimeMillis();
        String c, s = SERVER, from, to;
        if (StringUtils.isBlank(resource)) {
            c = uuid + "@" + SERVER;
        } else {
            c = uuid + "@" + SERVER + '/' + resource;
        }
        if (dir == MessageDirection.ClientToServer) {
            from = c;
            to = s;
        } else {
            from = s;
            to = c;
        }
        a(ScribeLogger.LOCAL_HOST).a(action).a(from).a(to).a(msgId).a(module).a(fromIp).a(TIME_FMT.format(now))
                .a(chid).a(StringUtils.isEmpty(topicOrAlias) ? clientIp : topicOrAlias)
                .a(messageCreateTimestamp == null? frontendIp: messageCreateTimestamp)
                .a(appId).a(packageName).a(os).a(model);
        if (!StringUtils.isBlank(sdkVersion)) {
            a(sdkVersion);
        } else {
            if (count != 1) {
                a(Integer.toString(count));
            }
        }
        sb.setLength(sb.length() - 1);
        this.built = true;
        return msg = sb.toString();
    }

    private UsageLog a(String s) {
        if (null != s) {
            /**
             * Make sure s.length <= MAX_FIELD_SIZE, and replace "," to REPLACE_COMMA_TO.
             */
            boolean addTailDots = false;
            int len = s.length();
            if (len > MAX_FIELD_SIZE) {
                addTailDots = true;
                len = MAX_FIELD_SIZE - 2;
            }
            for (int i=0; i<len; ++i) {
                char c = s.charAt(i);
                sb.append(c == ','? REPLACE_COMMA_TO: c);
            }
            if (addTailDots) {
                sb.append("..");
            }
        }
        sb.append(',');
        return this;
    }
    private UsageLog a(Object o) {
        return a(null == o? "": o.toString());
    }

    @Override
    public String toString() {
        return build();
    }
}
