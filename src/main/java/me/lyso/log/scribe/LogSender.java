/**
 * LogSender.java
 * [CopyRight]
 * @author leo [leoyonn@gmail.com]
 * @date Sep 9, 2013 3:13:06 PM
 */
package me.lyso.log.scribe;

import com.lmax.disruptor.EventHandler;
import me.lyso.perf.PerfCounter;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scribe.thrift.LogEntry;

import java.util.ArrayList;
import java.util.List;

/**
 * Event handler for reading log jobs from RingBuffer and send to scribe server.<BR>
 * If {@link #client} fails, retry for the first time, and double "retry-round-interval" in the following
 * rounds. Here 1 round in "retry-round-interval" means interval between 2 batch sends.
 * 
 * @author leo
 */
public class LogSender implements EventHandler<LogEvent<String>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogSender.class);
    /** batch send max size */
    protected static final int BATCH_SIZE = 1000;
    /** batch send max time to wait */
    protected static final long BATCH_INTERVAL_MS = 100;
    /** time interval between try reconnect to scribe-server */
    protected static final int RETRY_INTERVAL_COUNT_MAX = 1 << 8;
    protected static final int RETRY_INTERVAL_COUNT_MIN = 1;

    protected final String LOG_CATEGORY;

    private scribe.thrift.scribe.Iface client;
    private final List<LogEntry> buf;
    private int count = 0;
    private long lastSentTs = System.currentTimeMillis();
    private int retry = 1;
    private int nexRetry = RETRY_INTERVAL_COUNT_MIN;

    private final String zkPath;
    private final String scribeHost;
    private final int scribePort;

    private LogSender(String category, String zkPath, String host, int port) {
        this.LOG_CATEGORY = category;
        this.zkPath = zkPath;
        this.scribeHost = host;
        this.scribePort = port;

        this.buf = new ArrayList<LogEntry>(BATCH_SIZE) {
            private static final long serialVersionUID = 1L;
            {
                for (int i = 0; i < BATCH_SIZE; i++) {
                    add(new LogEntry(LOG_CATEGORY, ""));
                }
            }
        };
        refreshClient(null);
    }

    public LogSender(String category, String zkPath) {
        this(category, zkPath, null, 0);
    }

    public LogSender(String category, String host, int port) {
        this(category, null, host, port);
    }

    private boolean refreshClient(List<LogEntry> logs) {
        PerfCounter.count(PerfConstants.REFRESH_CLIENT, 1);
        LOGGER.info("^#Red.try-refresh-logsender: retry={}({})th.", retry, nexRetry);
        try {
            if (zkPath != null) {
                this.client = LogClientHelper.getScribeClientFromZookeeper(zkPath);
            } else if (scribeHost != null && scribePort > 0) {
                this.client = LogClientHelper.getScribeClient(scribeHost, scribePort);
            }
            if (logs != null) {
                client.Log(logs);
            }
            PerfCounter.count(PerfConstants.REFRESH_CLIENT_SUCCESS, 1);
            return true;
        } catch (Exception ex) {
            LOGGER.error("^#Red.init-logsender: can't get scribe client: {}", ex); // don't print stacktrace
            client = null;
            PerfCounter.count(PerfConstants.REFRESH_CLIENT_FAIL, 1);
            return false;
        }
    }

    @Override
    public void onEvent(final LogEvent<String> e, final long sequence, final boolean endOfBatch) throws Exception {
        String log = e.get();
        boolean nullLog = log == null;
        // count and ts would be reset in #send
        if (!nullLog) {
            buf.get(count++).setMessage(log);
        }

        if (count >= BATCH_SIZE) {
            PerfCounter.count(PerfConstants.SEND_FULL_BATCH, 1);
            send(buf);
        } else if (count > 0) {
            long ts = System.currentTimeMillis();
            if (nullLog || endOfBatch || ts - lastSentTs > BATCH_INTERVAL_MS) {
                PerfCounter.count(PerfConstants.SEND_DELAY_BATCH, 1);
                send(buf.subList(0, count));
            }
        }
    }

    /**
     * Batch send LogEntry list to scribe server.
     * 
     * @param toSend
     */
    private void send(List<LogEntry> toSend) {
        LOGGER.debug("^#Blue.log-scribe: sending {} / {} logs.", count, toSend.size());
        if (client != null) {
            try {
                client.Log(toSend);
                retry = 0;
                nexRetry = RETRY_INTERVAL_COUNT_MIN;
                PerfCounter.count(PerfConstants.SEND_SUCCESS, 1);
            } catch (Exception ex) {
                LOGGER.error("^#Red.log-scribe-exception: {}", ex);
                client = null;
                PerfCounter.count(PerfConstants.SEND_FAIL, 1);
            }
        }
        if (client == null) {
            if ((++retry) >= nexRetry) {
                if (refreshClient(toSend)) {
                    nexRetry = RETRY_INTERVAL_COUNT_MIN;
                } else {
                    if (nexRetry < RETRY_INTERVAL_COUNT_MAX) {
                        nexRetry <<= 1;
                    }
                }
                retry = 0;
            }
        }
        count = 0;
        lastSentTs = System.currentTimeMillis();
    }

    /**
     * Batch send LogEntry list to scribe server synchronously.
     * 
     * @param toSend
     * @return
     */
    public boolean sendSync(List<LogEntry> toSend) {
        try {
            if (client != null) {
                client.Log(toSend);
                return true;
            }
        } catch (TException ex) {
            LOGGER.error("^#Red.log-scribe-exception: {}", toSend, ex);
            client = null;
        }

        return false;
    }

    public boolean isValid() {
        return client != null;
    }
}
