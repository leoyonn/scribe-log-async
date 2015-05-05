/**
 * ScribeLogger.java
 * [CopyRight]
 * @author leo [leoyonn@gmail.com]
 * @date Sep 2, 2013 4:47:28 PM
 */
package me.lyso.log.scribe;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scribe.thrift.LogEntry;

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import me.lyso.perf.PerfCounter;

/**
 * async scribe logger.
 * <ol>
 * <li>1. use a disrupter(ringbuffer) of {@link #RING_SIZE} as queue for buffering incomming log request;
 * <li>2. use a thread {@link #logSender} with buffer for batch send logs to scribe server;
 * <li>3. use a timer(scheduled excecutor) {@link #logSendTimer} to ping {@link #logSender}, to resolve
 * problem of sending un-full buffer rapidly.
 * </ol>
 * <ol>
 * work flow:
 * <li>1. use {@link LogEvent} to append a log request;
 * <li>2. this log function will tryPublish to disruptor's ringbuffer, will use log4j if ringbuffer is full;
 * <li>3. {@link #logSender} consume ringbuffer, and batch send logs when:
 * <ol>
 * <li>3.1. {@link #logSender} 's little buffer is full;
 * <li>3.2. time elapse more than {@link LogSender#BATCH_INTERVAL_MS};
 * <li>3.3. {@link #logSendTimer} pinged.
 * </ol>
 * <li>4. will send all left in buffer on shutdown.
 * </ol>
 *
 * @author leo
 */
public class ScribeLogger {
    public static final String LOCAL_HOST;
    private static final Logger LOGGER = LoggerFactory.getLogger(ScribeLogger.class);
    private static final ConcurrentHashMap<String, ScribeLogger> loggers = new ConcurrentHashMap<String, ScribeLogger>();
    private static final int RING_SIZE = 1 << 12;

    static {
        String host = "";
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception ex) {
            host = "bad-host";
            LOGGER.error("^#Red.get-localhost", ex);
        }
        LOCAL_HOST = host;
        LOGGER.info("^#Blue.init-step1: init local host: {}.", LOCAL_HOST);
    }

    private final ExecutorService logSendExecutor = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService logSendTimer = Executors.newSingleThreadScheduledExecutor();
    private final LogSender logSender;
    private Disruptor<LogEvent<String>> disruptor;
    private RingBuffer<LogEvent<String>> ringBuffer;

    /**
     * get a ScribeLogger with #category and connect to servers described by #logServersZkPath
     * 
     * @param category
     * @param logServersZkPath
     * @return
     */
    public static ScribeLogger get(String category, String logServersZkPath) {
        String k = category + "@" + logServersZkPath;
        ScribeLogger logger = loggers.get(k);
        if (logger == null) {
            logger = new ScribeLogger(new LogSender(category, logServersZkPath));
            ScribeLogger existed = loggers.putIfAbsent(k, logger);
            if (existed != null) {
                return existed;
            }
        }
        return logger;
    }

    /**
     * get a ScribeLogger with #category and connect to host:port
     *
     * @param category
     * @param host
     * @param port
     * @return
     */
    public static ScribeLogger get(String category, String host, int port) {
        String k = category + "@" + host + ":" + port;
        ScribeLogger logger = loggers.get(k);
        if (logger == null) {
            logger = new ScribeLogger(new LogSender(category, host, port));
            ScribeLogger existed = loggers.putIfAbsent(k, logger);
            if (existed != null) {
                return existed;
            }
        }
        return logger;
    }

    private ScribeLogger(LogSender logSender) {
        LOGGER.info("^#Blue.init-step0: begin initialize...");
        this.logSender = logSender;
        initDisruptor();
        logSendTimer.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                log(null);
            }
        }, LogSender.BATCH_INTERVAL_MS * 5, LogSender.BATCH_INTERVAL_MS * 5, TimeUnit.MILLISECONDS);
    }

    /**
     * if needed, send the last message when shutdown.
     */
    public void onShutdown(String message) {
        LOGGER.info("^#Green.shutdown: scribe logger exited gracefully");
        try {
            logSender.onEvent(new LogEvent<String>().set(message), -1, true);
        } catch (Exception ex) {
            LOGGER.error("^#Red.shutdown: scribe logger exception during exit", ex);
        }
    }

    /**
     * init disruptor for buffering log sending jobs.
     */
    @SuppressWarnings("unchecked")
    private void initDisruptor() {
        disruptor = new Disruptor<LogEvent<String>>(LogEvent.STRING_EVENT_FACTORY, RING_SIZE,
                logSendExecutor, ProducerType.SINGLE, new SleepingWaitStrategy());
        disruptor.handleEventsWith(logSender);
        ringBuffer = disruptor.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    disruptor.shutdown();
                    logSendExecutor.shutdown();
                } catch (Throwable t) {
                    LOGGER.error("^#Red.shutdown: scribe logger exception during exit", t);
                }
            }
        });
        LOGGER.info("^#Blue.init-step4: init disruptor: {}", disruptor);
    }

    /**
     * send a scribe log synchronously.
     *
     * @param message
     */
    public boolean logSync(String message) {
        return logSender.sendSync(Arrays.asList(new LogEntry(logSender.LOG_CATEGORY, message)));
    }

    /**
     * send a scribe log asynchronously.
     * 
     * @param message
     */
    public boolean log(String message) {
        long sequence = -1;
        try {
            sequence = ringBuffer.tryNext();
        } catch (InsufficientCapacityException e) {
            PerfCounter.count(PerfConstants.RING_FAIL, 1);
            return false;
        }
        try {
            ringBuffer.get(sequence).set(message);
        } finally {
            ringBuffer.publish(sequence);
        }
        return true;
    }
}
