/**
 * LogEvent.java
 * [CopyRight]
 * @author leo [leoyonn@gmail.com]
 * @date Sep 9, 2013 3:14:06 PM
 */

package me.lyso.log.scribe;

import com.lmax.disruptor.EventFactory;

/**
 * event wrapper for disrupter
 * 
 * @param <T>
 * @author leo
 */
public class LogEvent<T> {
    private T v;

    public LogEvent<T> set(T v) {
        this.v = v;
        return this;
    }

    public T get() {
        return v;
    }

    @Override
    public String toString() {
        return v == null ? "null" : v.toString();
    }

    public final static EventFactory<LogEvent<String>> STRING_EVENT_FACTORY = new EventFactory<LogEvent<String>>() {
        @Override
        public LogEvent<String> newInstance() {
            return new LogEvent<String>();
        }
    };

}
