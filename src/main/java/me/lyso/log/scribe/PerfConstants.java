/**
 * PerfConstants.java
 * [CopyRight]
 * @author leo [leoyonn@gmail.com]
 * @date Mar 4, 2014 10:47:19 AM
 */
package me.lyso.log.scribe;

/**
 * Perf-counter keys for Scribe logger.
 * 
 * @author leo
 */
public interface PerfConstants {
    String REFRESH_CLIENT = "counter~scribelog~reconnect";
    String REFRESH_CLIENT_SUCCESS = "counter~scribelog~reconnect~success";
    String REFRESH_CLIENT_FAIL = "counter~scribelog~reconnect~fail";
    String SEND_FULL_BATCH = "counter~scribelog~send~fullbatch";
    String SEND_DELAY_BATCH = "counter~scribelog~send~delaybatch";
    String SEND_SUCCESS = "counter~scribelog~send~success";
    String SEND_FAIL = "counter~scribelog~send~fail";
    String RING_FAIL = "counter~scribelog~getring~fail";
}
