/**
 * UsageLogger.java
 * [CopyRight]
 * @author leo [leoyonn@gmail.com]
 * @date Sep 2, 2013 4:47:28 PM
 */
package me.lyso.log.usage;

import me.lyso.perf.PerfCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.lyso.log.scribe.ScribeLogger;

/**
 * Async usage logger, use {@link ScribeLogger}.
 *
 * @author leo
 */
public class UsageLogger {
    private static final Logger LOGGER = LoggerFactory.getLogger(UsageLogger.class);
    private final ScribeLogger slogger;
    private String category = "usage";
    private String module = "m:unset";

    public UsageLogger(String category, String logServersZkPath) {
        this.category = category;
        slogger = ScribeLogger.get(this.category, logServersZkPath);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    String message = new UsageLog().setDir(MessageDirection.ServerToClient)
                            .setAction("shutdown").setModule(module()).build();
                    slogger.onShutdown(message);
                } catch (Throwable t) {
                    LOGGER.error("^#Red.shutdown: usage logger exception during exit", t);
                }
            }
        });
    }

    public UsageLogger setModule(String module) {
        this.module = module;
        return this;
    }

    public String module() {
        return module;
    }

    public String category() {
        return category;
    }

    public void log(MessageDirection dir, String action, String uuid, String resource,
                    String msgId, String appId, String packageName) {
        log(dir, action, uuid, resource, msgId, appId, packageName, "", null, 1);
    }

    public void log(MessageDirection dir, String action, String uuid, String resource,
                    String msgId, String appId, String packageName, String topicOrAlias, Long messageCreateTimeStamp) {
        log(dir, action, uuid, resource, msgId, appId, packageName, topicOrAlias, messageCreateTimeStamp, 1);
    }
    /**
     * Log a usage behavior asynchronously.
     *
     * @param dir
     * @param action
     * @param uuid
     * @param resource
     * @param msgId
     * @param appId
     * @param packageName
     * @param topicOrAlias
     * @param  messageCreateTimeStamp
     */
    public void log(MessageDirection dir, String action, String uuid, String resource,
        String msgId, String appId, String packageName, String topicOrAlias, Long messageCreateTimeStamp, int count) {
        log(new UsageLog()
                .setModule(module)
                .setAction(action)
                .setDir(dir)
                .setUuid(uuid)
                .setResource(resource)
                .setMsgId(msgId)
                .setAppId(appId)
                .setPackageName(packageName)
                .setTopicOrAlias(topicOrAlias)
                .setMessageCreateTimestamp(messageCreateTimeStamp)
                .setCount(count));
    }

    /**
     * Log a usage behavior asynchronously.
     *
     * @param record
     */
    public void log(UsageLog record) {
        long ts = System.currentTimeMillis();
        slogger.log(record.build());
        PerfCounter.count("UsageLogger.log", 1, System.currentTimeMillis() - ts);
    }

    public void logSync(MessageDirection dir, String action, String uuid, String resource,
                    String msgId, String appId, String packageName) {
        logSync(dir, action, uuid, resource, msgId, appId, packageName, "", null);
    }

    /**
     * Log a usage behavior synchronously.
     *
     * @param dir
     * @param action
     * @param uuid
     * @param resource
     * @param msgId
     * @param appId
     * @param packageName
     * @param topicOrAlias
     * @param messageCreateTimeStamp
     */
    public void logSync(MessageDirection dir, String action, String uuid, String resource,
            String msgId, String appId, String packageName, String topicOrAlias, Long messageCreateTimeStamp) {
        logSync(new UsageLog()
                .setModule(module)
                .setAction(action)
                .setDir(dir)
                .setUuid(uuid)
                .setResource(resource)
                .setMsgId(msgId)
                .setAppId(appId)
                .setPackageName(packageName)
                .setTopicOrAlias(topicOrAlias)
                .setMessageCreateTimestamp(messageCreateTimeStamp));
    }

    /**
     * Log a usage behavior synchronously.
     * @param record
     */
    public void logSync(UsageLog record) {
        long ts = System.currentTimeMillis();
        slogger.logSync(record.build());
        PerfCounter.count("UsageLogger.logSync", 1, System.currentTimeMillis() - ts);
    }
}
