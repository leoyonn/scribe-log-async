/**
 * ScribeLogTest.java
 * [CopyRight]
 * @author leo [leoyonn@gmail.com]
 * @date Mar 3, 2014 8:30:30 PM
 */
package me.lyso.log.scribe;

import org.junit.Test;

/**
 * @author leo
 */
public class ScribeLogTest {
    @Test
    public void test() throws InterruptedException {
        ScribeLogger logger = ScribeLogger.get("usage", "10.237.12.17", 1463);
        for (int i = 0; i < 100; i ++) {
            logger.log("1'th batch log sending" + i);
            Thread.sleep(10);
        }
        Thread.sleep(1000);
        for (int i = 0; i < 100; i ++) {
            logger.log("2'th batch log sending" + i);
            Thread.sleep(10);
        }
        Thread.sleep(1000);
        for (int i = 0; i < 100; i ++) {
            logger.log("3'th batch log sending" + i);
            Thread.sleep(10);
        }
        Thread.sleep(1000);
        for (int i = 0; i < 100; i ++) {
            logger.log("4'th batch log sending" + i);
            Thread.sleep(10);
        }
        Thread.sleep(1000);
        for (int i = 0; i < 100; i ++) {
            logger.log("5'th batch log sending" + i);
            Thread.sleep(10);
        }
        for (int i = 0; i < 1000; i ++) {
            logger.log("6'th batch log sending" + i);
            Thread.sleep(10);
        }
        Thread.sleep(1000);
    }

}
