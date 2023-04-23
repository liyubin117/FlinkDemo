package state

/**
 *
 * @param sid      基站ID
 * @param callOut  主叫号码
 * @param callIn   被叫号码
 * @param callType 通话类型eg:呼叫失败(fail)，占线(busy),拒接（barring），接通(success):
 * @param callTime 呼叫时间戳，精确到毫秒
 * @param duration 通话时长 单位：秒
 */
case class StationLog(sid: String, callOut: String, callIn: String, callType: String, callTime: Long, duration: Long)