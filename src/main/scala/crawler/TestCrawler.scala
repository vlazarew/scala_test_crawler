package crawler

import enums.{LogLevel, MessageType}
import helpers.MessageWriter
import org.json4s.JNothing
import org.json4s.JsonDSL._
import org.json4s.jackson.compactJson
import org.sellmerfud.optparse.OptionParser
import scalaj.http.Http

import java.nio.file.{Files, Path}
import java.time.Instant
import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Random


case class TestCrawler(requests: Int = 1,
                       items: Int = 100,
                       errors: Int = 0,
                       workTime: Int = 0,
                       fail: Boolean = false,
                       fieldFrequency: Int = 100,
                       getCrawlerNames: Boolean = false)

object TestCrawler extends App {
  val name = "scala_test_crawler"

  val DEMO_URL = "https://demo-site.at.ispras.ru/"

  private val fileName = "outputFile.jsonl"

  val random = new Random

  lazy val outputFile: Path = {
    Files.deleteIfExists(Path.of(fileName))
    Files.createFile(Path.of(fileName))
  }

  val config = getConfig

  var completeRequests = 0
  var crawledItems = 0
  var crawledErrors = 0
  val delay = (config.workTime * 1000) / (config.requests + config.items + config.errors)

  def weightedFreq[A](freq: mutable.LinkedHashMap[A, Int]): A = {
    require(freq.forall { case (_, f) => f >= 0 })
    require(freq.values.sum > 0)

    @tailrec
    def weighted(todo: Iterator[(A, Int)], rand: Int, accum: Int = 0): A = todo.next match {
      case (s, i) if rand < (accum + i) => s
      case (_, i) => weighted(todo, rand, accum + i)
    }

    weighted(freq.toIterator, scala.util.Random.nextInt(freq.values.sum))
  }

  if (config.getCrawlerNames) {
    println(name)
  } else {
    initCrawler(config)
    processEvents()
  }

  private def getConfig: TestCrawler = {
    new OptionParser[TestCrawler] {
      optl[Int]("", "--requests=REQUESTS_COUNT") { (value, config) => config.copy(requests = value.getOrElse(1)) }
      optl[Int]("", "--items=ITEMS_COUNT") { (value, config) => config.copy(items = value.getOrElse(100)) }
      optl[Int]("", "--errors=ERRORS_COUNT") { (value, config) => config.copy(errors = value.getOrElse(0)) }
      optl[Int]("", "--workTime=WORK_TIME") { (value, config) => config.copy(workTime = value.getOrElse(0)) }
      optl[String]("", "--fail=FAIL") { (value, config) =>
        config.copy(fail = try {
          value match {
            case Some(value) => value.toBoolean
            case None => false
          }
        } catch {
          case _: Throwable => false
        })
      }
      optl[Int]("", "--fieldFrequency=FIELD_FREQUENCY") { (value, config) => config.copy(fieldFrequency = value.getOrElse(100)) }
      flag("", "--crawlerNames") { value => value.copy(getCrawlerNames = true) }
    }.parse(args, TestCrawler())
  }

  def initCrawler(config: TestCrawler): Unit = {

    val item = if (config.fail) {
      ("message" -> "Critical failure occurred") ~ ("level" -> LogLevel.CRITICAL.toString) ~ ("timestamp" -> Instant.now().getEpochSecond)
      throw new Exception("Critical failure occurred")
    } else {
      ("message" -> s"Spider run with config: $config") ~ ("level" -> LogLevel.INFO.toString) ~ ("timestamp" -> Instant.now().getEpochSecond)
    }

    MessageWriter.writeMessage(MessageType.LOG, compactJson(item))
  }

  def makeTestRequest(): Unit = {
    completeRequests += 1
    val request = Http(DEMO_URL)
    val startTime = Instant.now().getNano
    val response = request.asString

    val item = ("_url" -> DEMO_URL) ~ ("_timestamp" -> startTime) ~ ("method" -> request.method) ~
      ("status" -> response.code) ~ ("duration" -> ((Instant.now().getNano - startTime) / 10e9)) ~
      ("rs" -> response.body.getBytes().length)
    MessageWriter.writeMessage(MessageType.REQ, compactJson(item))

    processEvents()
  }

  def makeTestItem(): Unit = {
    crawledItems += 1
    val countOfAttachments = random.nextInt(4)

    val attachments = for (_ <- 1 to countOfAttachments)
      yield ("path" -> "s3://sitemaps/some") ~ ("filename" -> "some") ~ ("checksum" -> "68b329da9893e34099c7d8ad5cb9c940")

    val value = if (random.nextInt(100) >= (100 - config.fieldFrequency)) Some("value" -> s"Test item #$crawledItems") else None

    val item = ("_url" -> DEMO_URL) ~ ("_timestamp" -> Instant.now().getEpochSecond) ~ ("_attachments" -> attachments) ++
      (if (value.isDefined) value else JNothing)
    MessageWriter.writeMessage(MessageType.ITM, compactJson(item))

    processEvents()
  }

  def makeTestError(): Unit = {
    crawledErrors += 1
    val item = ("message" -> s"Test error #$crawledErrors") ~ ("level" -> LogLevel.ERROR.toString) ~ ("timestamp" -> Instant.now().getEpochSecond)
    MessageWriter.writeMessage(MessageType.LOG, compactJson(item))
    processEvents()
  }

  def processEvents(): Unit = {
    if (delay > 0) {
      Thread.sleep(delay)
    }

    getNextEvent match {
      case Some(value) => value()
      case None => MessageWriter.writeMessage(MessageType.FIN, s"Successful finished")
    }
  }

  private def getNextEvent = {
    val possibleEvents = Seq(
      if (completeRequests < config.requests) Some(() => makeTestRequest()) else None,
      if (crawledItems < config.items) Some(() => makeTestItem()) else None,
      if (crawledErrors < config.errors) Some(() => makeTestError()) else None
    ).filter(_.isDefined)

    possibleEvents.length match {
      case length if length > 0 => random.shuffle(possibleEvents).head
      case _ => None
    }

  }
}
