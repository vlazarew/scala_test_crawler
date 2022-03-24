package enums

object MessageType extends Enumeration {
  type MessageType = Value
  /**
   * Item - tcontroller
   * Log - elastic LogsIndex
   * Request - elastic RequestsIndex
   * Stats - elastic Metrics (1 per minute or each 100 items)
   * Finish - elastic
   * Media - minio
   */
  val Item, Log, Request, Stats, Finish, Media = Value
}
