package enums

object LogLevel extends Enumeration {
  type LogLevel = Value
  val DEBUG, INFO, WARNING, ERROR, CRITICAL = Value
}
