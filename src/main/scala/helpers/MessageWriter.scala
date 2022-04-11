package helpers

import crawler.TestCrawler.outputFile
import enums.MessageType
import enums.MessageType.MessageType
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue
import org.json4s.jackson.Serialization

import java.io.FileWriter

object MessageWriter {

  implicit val formats: DefaultFormats.type = DefaultFormats

  private def getMessage(messageType: MessageType, content: JValue) = {
    Serialization.write(messageType match {
      case MessageType.Finish => messageType.toString -> ("outcome" -> content)
      case MessageType.Media => messageType.toString -> ("value" -> content)
      case _ => messageType.toString -> content
    })
  }

  def writeMessage(messageType: MessageType, content: JValue): Unit = {
    val message = getMessage(messageType, content)
    val writer = new FileWriter(new java.io.File(outputFile.getFileName.toString), true)

    writer.write(message + "\n")
    println(message)
    writer.close()
  }
}
