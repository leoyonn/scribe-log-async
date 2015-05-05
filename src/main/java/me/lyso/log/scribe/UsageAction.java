/**
 *
 * UsageAction.java
 * @date 14-11-3 下午5:15
 * @author leo [leoyonn@gmail.com]
 * [CopyRight] All Rights Reserved.
 */

package me.lyso.log.scribe;

/**
 * Actions for messaging usage log.
 *
 * @author leo
 */
public enum UsageAction {
    // user login attempt
    login,
    // user login successfully
    user_login_succ,
    // user failed to login
    user_login_fail,
    // user failed to login
    user_logout,
    // message
    msg,
    // message deliver (from server to client), used only for muc
    dlvr,

    // message sent ack
    msg_sent,
    // received message acked by destination
    msg_recv,
    // read message acked by destination
    msg_read,
    msg_del,
    msg_delthread,
    msg_read_gc,
    msg_ack,
    msg_sync,

    // group chat message
    group_msg,
    //group chat received by client
    group_recv,

}
