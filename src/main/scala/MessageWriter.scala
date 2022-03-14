import MessageType.MessageType
import TestCrawler.outputFile
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import java.io.FileWriter

object MessageWriter {

  implicit val formats: DefaultFormats.type = DefaultFormats

  private def getMessage(messageType: MessageType, content: String) = {
    Serialization.write(messageType match {
      case MessageType.FIN => messageType.toString -> ("outcome" -> content)
      case _ => messageType.toString -> content
    })
  }

  def writeMessage(messageType: MessageType, content: String): Unit = {
    val message = getMessage(messageType, content)
    val writer = new FileWriter(new java.io.File(outputFile.getFileName.toString), true)

    writer.write(message + "\n")
    println(message)
    writer.close()
  }
}
