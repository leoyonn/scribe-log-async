/**
 *
 * ScribePerf.java
 * @date 14-11-4 下午2:29
 * @author leo [leoyonn@gmail.com]
 * [CopyRight] All Rights Reserved.
 */

package me.lyso.log.scribe;

import me.lyso.mampa.actor.*;
import me.lyso.mampa.actor.router.OneActorRouter;
import me.lyso.mampa.actor.router.OneFsmRouter;
import me.lyso.mampa.event.IAction;
import me.lyso.mampa.event.IEventType;
import me.lyso.mampa.fsm.AbstractRuleSet;
import me.lyso.mampa.fsm.IFsmType;
import me.lyso.mampa.fsm.IFsmType.DefaultFsmType;
import me.lyso.mampa.fsm.IStateType;
import me.lyso.mampa.fsm.State;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Mampa actor for scribe-perfcounter.
 * Hold only 1 instance in a process.
 *
 * @author leo
 */
public class ScribePerf {
    private static final FastDateFormat TimeFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    private static final Logger logger = LoggerFactory.getLogger(ScribePerf.class);
    private static final long DumpDelayInMins = 15;
    private static final long MillisPerMin = 1000 * 60L;
    private static final long MillisPerHour = MillisPerMin * 60L;

    private final ScribeLogger perfLogger;
    private final ActorGroup<String, Integer> actor;
    private final IActorGroup xmqActor;
    private final String host;
    private final String instanceName;

    public ScribePerf(ScribeLogger perfLogger, IActorGroup xmqActor, String host, String instanceName) {
        this.perfLogger = perfLogger;
        this.xmqActor = xmqActor;
        this.host = host;
        this.instanceName = instanceName;
        this.actor = new ActorGroup<String, Integer>(ActorConfig.builder(
                "Scribe-Perf", OneActorRouter.instance(), new OneFsmRouter<Integer>(), new RuleSet())
                .ringSizesPerActor(new int[]{1 << 14}).priorityConfigs(new int[]{1024})
                .actorNumber(1).initialFsmCount(1).maxFsmCount(1).build()).start();
    }

    public static enum ScribePerfName {
        LoginSucc, LoginFail, Send, Connect,
    }

    private class Value {
        private int chid;
        private int loginSucc;
        private int loginFail;
        private int send;
        private int connectRetry;
        private String start;
        private String end;

        public Value(int chid) {
            this.chid = chid;
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }

        public void inc(ScribePerfName name) {
            int v = -1;
            switch(name) {
                case LoginSucc: v = loginSucc ++;   break;
                case LoginFail: v = loginFail ++;   break;
                case Send:      v = send++;         break;
                case Connect:   v = connectRetry++; break;
                default: break;
            }
            logger.debug("Inc {} to {}", name, v);
        }

        public Value dump() {
            StringBuilder sb = new StringBuilder();
            String interval = sb.append(host).append(',').append(start).append(',').append(end).append(',').toString();
            sb.setLength(0);
            sb.append("CONNECT_TRY_PER_15_MIN,").append(interval).append(connectRetry).append(",0");
            perfLogger.log(sb.toString());
            
            sb.setLength(0);
            String tail = sb.append(',').append(chid).append(',').append(instanceName).toString();
            
            sb.setLength(0);
            String loginSuccLog = sb.append("LOGIN_SUCC_PER_15_MIN,").append(interval).append(loginSucc).append(tail).toString();
            perfLogger.log(loginSuccLog);

            sb.setLength(0);
            String loginFailLog = sb.append("LOGIN_FAIL_PER_15_MIN,").append(interval).append(loginFail).append(tail).toString();
            perfLogger.log(loginFailLog);

            sb.setLength(0);
            String msgLog = sb.append("MSG_PER_15_MIN,").append(interval).append(send).append(tail).toString();
            perfLogger.log(msgLog);

            sb.setLength(0);
            String onlineLog = sb.append("ONLINE_PER_15_MIN,").append(interval).append(xmqActor.fsmSize()).append(tail).toString();
            perfLogger.log(onlineLog);
            
            logger.info("Dumped {}", loginSuccLog);
            logger.info("Dumped {}", loginFailLog);
            logger.info("Dumped {}", msgLog);
            logger.info("Dumped {}", onlineLog);
            return this;
        }

        public Value start(String start) {
            this.start = start;
            return this;
        }

        public Value next(String next) {
            this.end = next;
            return this;
        }
    }

    private static enum StateType implements IStateType {
        Running, Stop,
    }

    private static enum EventType implements IEventType {
        Inc, Dump, Timeout
    }

    public boolean inc(int chid, ScribePerfName name) {
        return actor.tell(null, DefaultFsmType.One, chid, EventType.Inc, name);
    }

    private class RuleSet extends AbstractRuleSet<Integer, Value> {
        public RuleSet() {
            super(StateType.values().length, EventType.values().length, EventType.Timeout, StateType.Stop);
        }

        @Override
        public boolean init(State<Value> state, Integer chid, IEventType _etype, Object _data, IActor<Integer> master) {
            logger.info("Init Scribe Perf FSM: by {} {}", _etype, _data);
            state.type(StateType.Running).value(new Value(chid));
            scheduleNextDump(state.value(), chid, master);
            return true;
        }

        private void scheduleNextDump(Value value, int chid, IActor<Integer> master) {
            long    now = System.currentTimeMillis(), mins = now / MillisPerMin, hours = now / MillisPerHour,
                    start = (mins % 60 / DumpDelayInMins * DumpDelayInMins + hours * 60L) * MillisPerMin,
                    next = start + DumpDelayInMins * MillisPerMin,
                    delay = next - now;
            value.start(TimeFormat.format(start)).next(TimeFormat.format(next));
            logger.info("Schedule next dump: now: {}, start: {}, next: {}, delay: {}:{}",
                    now, value.start, value.end, delay / 1000 / 60, delay / 1000 % 60);
            master.dealAfter(DefaultFsmType.One, chid, EventType.Dump, null, delay, TimeUnit.MILLISECONDS);
        }

        @Override
        protected void buildRules() {
            addRule(EventType.Inc, new IAction<Integer, Value>() {
                @Override
                public NextState exec(State<Value> state, IFsmType fsmType, Integer chid, IEventType etype, Object data,
                        IActor<Integer> master) throws Exception {
                    state.value().inc((ScribePerfName) data);
                    return nextState(master, state.type());
                }
            });

            addRule(EventType.Dump, new IAction<Integer, Value>() {
                @Override
                public NextState exec(State<Value> state, IFsmType fsmType, Integer chid, IEventType etype, Object data,
                        IActor<Integer> master) throws Exception {
                    scheduleNextDump(state.value().dump(), chid, master);
                    return nextState(master, state.type());
                }
            });
        }

        @Override
        protected IAction<Integer, Value> buildDefaultRule() {
            return new IAction<Integer, Value>() {
                @Override
                public NextState exec(State<Value> state, IFsmType fsmType, Integer chid, IEventType etype, Object data,
                        IActor<Integer> master) throws Exception {
                    logger.warn("Invalid Scribe Perf Event on state: {} : {}/{}", state.type(), etype, data);
                    return nextState(master, state.type());
                }
            };
        }
    }
}
