package crawler

import akka.actor.ActorSystem
import enums.{LogLevel, MessageType}
import helpers.{ArgsParser, MessageWriter}
import org.json4s.JNothing
import org.json4s.JsonDSL._
import scalaj.http.Http

import java.io._
import java.nio.file.{Files, Path}
import java.time.{Duration, Instant}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.io.Source
import scala.reflect.runtime.universe.typeOf
import scala.util.Random

case class Metrics(time: Long = 0,
                   itemsCount: Long = 0,
                   requestsCount: Long = 0,
                   errorsCount: Long = 0)

case class TestCrawler(requests: Int = 1,
                       items: Int = 100,
                       errors: Int = 0,
                       workTime: Int = 60,
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

  Runtime.getRuntime().addShutdownHook(new Thread {
    override def run = {
     //println("---------SHUTDOWN HOOK!!!!---------")
      val pw = new PrintWriter(new File("test.txt" ))
      pw.write(s"${metrics.itemsCount}")
      pw.close
    }
  })

  var metrics = Metrics()

  val startCrawlTime = Instant.now()

  val actorSystem = ActorSystem()
  val scheduler = actorSystem.scheduler
  implicit val executor: ExecutionContextExecutor = actorSystem.dispatcher

  val metricsNotifier = scheduler.scheduleAtFixedRate(initialDelay = Duration.ofMinutes(1), interval = Duration.ofMinutes(1),
    runnable = () => {
      val metricsInfo = createMetricsBody(Instant.now())
      MessageWriter.writeMessage(MessageType.Stats, metricsInfo)
    }, executor)

  private val crawlerDefinition = typeOf[TestCrawler].members.withFilter(!_.isMethod)
    .map(el => (el.name.toString.trim, el.typeSignature)).toMap

  val crawlerArgs = ArgsParser.parse(args, crawlerDefinition)
  val config = getConfig(crawlerArgs)

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

  Future {
    if (config.getCrawlerNames) {
      println(name)
    } else {
      initCrawler(config)
      processEvents()
    }

    metricsNotifier.cancel()
    System.exit(0)
  }

  private def getConfig(crawlerArgs: Map[String, Any]): TestCrawler = {
    TestCrawler(
      requests = crawlerArgs.getOrElse("requests", 1).asInstanceOf[Int],
      items = crawlerArgs.getOrElse("items", 100).asInstanceOf[Int],
      errors = crawlerArgs.getOrElse("errors", 0).asInstanceOf[Int],
      workTime = crawlerArgs.getOrElse("workTime", 0).asInstanceOf[Int],
      fail = crawlerArgs.getOrElse("fail", false).asInstanceOf[Boolean],
      fieldFrequency = crawlerArgs.getOrElse("fieldFrequency", 100).asInstanceOf[Int],
      getCrawlerNames = crawlerArgs.getOrElse("getCrawlerNames", false).asInstanceOf[Boolean]
    )
  }

  def initCrawler(config: TestCrawler): Unit = {

    val item = if (config.fail) {
      ("message" -> "Critical failure occurred") ~ ("level" -> LogLevel.CRITICAL.toString) ~ ("timestamp" -> Instant.now().toEpochMilli)
      MessageWriter.writeMessage(MessageType.Finish, s"Critical Error")

      val failTime = Instant.now()
      val metricsInfo = createMetricsBody(failTime, isCritical = true)

      MessageWriter.writeMessage(MessageType.Stats, metricsInfo)
      throw new Exception("Critical failure occurred")
    } else {
      ("message" -> s"Spider run with config: $config") ~ ("level" -> LogLevel.INFO.toString) ~ ("timestamp" -> Instant.now().toEpochMilli)
    }


    val bufferedSource = Source.fromFile("test.txt")
    for (line <- bufferedSource.getLines) {
      println(f"Previous itemsCount: ${line}")
    }
    bufferedSource.close

    MessageWriter.writeMessage(MessageType.Log, item)
  }

  private def createMetricsBody(finishTime: Instant, isCritical: Boolean = false) = {
    ("elapsed_time_seconds" -> (finishTime.getEpochSecond - startCrawlTime.getEpochSecond)) ~ ("item_scraped_count" -> metrics.itemsCount) ~
      ("requests_count" -> metrics.requestsCount) ~ ("log_count_ERROR" -> metrics.errorsCount) ~ ("log_count_CRITICAL" -> (if (isCritical) 1 else 0)) ~
      ("_timestamp" -> finishTime.toEpochMilli)
  }

  def makeTestRequest(): Unit = {
    synchronized {
      metrics = metrics.copy(time = Instant.now().getEpochSecond - startCrawlTime.getEpochSecond, metrics.itemsCount, requestsCount = metrics.requestsCount + 1, errorsCount = metrics.errorsCount)
    }
    val request = Http(DEMO_URL)
    val startTime = Instant.now().toEpochMilli
    val response = request.asString

    val item = ("_url" -> DEMO_URL) ~ ("_timestamp" -> startTime) ~ ("method" -> request.method) ~
      ("status" -> response.code) ~ ("duration" -> ((Instant.now().toEpochMilli - startTime) / 10e6)) ~
      ("response_size" -> response.body.getBytes().length)
    MessageWriter.writeMessage(MessageType.Request, item)

    processEvents()
  }

  def makeTestItem(): Unit = {
    synchronized {
      metrics = metrics.copy(time = Instant.now().getEpochSecond - startCrawlTime.getEpochSecond, metrics.itemsCount + 1, requestsCount = metrics.requestsCount, errorsCount = metrics.errorsCount)
    }
    val countOfAttachments = random.nextInt(4)

    val attachments = for (_ <- 1 to countOfAttachments)
      yield ("path" -> "s3://sitemaps/some") ~ ("filename" -> "some") ~ ("checksum" -> "68b329da9893e34099c7d8ad5cb9c940")

    val value = if (random.nextInt(100) >= (100 - config.fieldFrequency)) Some("value" -> s"Test item #${metrics.itemsCount}") else None

    val item = ("_url" -> DEMO_URL) ~ ("_timestamp" -> Instant.now().toEpochMilli) ~ ("_attachments" -> attachments) ++
      (if (value.isDefined) value else JNothing)
    MessageWriter.writeMessage(MessageType.Item, item)

    processEvents()
  }

  def makeTestError(): Unit = {
    synchronized {
      metrics = metrics.copy(time = Instant.now().getEpochSecond - startCrawlTime.getEpochSecond, metrics.itemsCount, requestsCount = metrics.requestsCount, errorsCount = metrics.errorsCount + 1)
    }
    val item = ("message" -> s"Test error #${metrics.errorsCount}") ~ ("level" -> LogLevel.ERROR.toString) ~ ("timestamp" -> Instant.now().toEpochMilli)
    MessageWriter.writeMessage(MessageType.Log, item)
    processEvents()
  }

  def processEvents(): Unit = {
    if (delay > 0) {
      Thread.sleep(delay)
    }

    getNextEvent match {
      case Some(value) => value()
      case None =>
        val metricsInfo = createMetricsBody(Instant.now())
        MessageWriter.writeMessage(MessageType.Stats, metricsInfo)
        MessageWriter.writeMessage(MessageType.Finish, s"Successful finished")
    }
  }

  private def getNextEvent = {
    val possibleEvents = Seq(
      if (metrics.requestsCount < config.requests) Some(() => makeTestRequest()) else None,
      if (metrics.itemsCount < config.items) Some(() => makeTestItem()) else None,
      if (metrics.errorsCount < config.errors) Some(() => makeTestError()) else None
    ).filter(_.isDefined)

    possibleEvents.length match {
      case length if length > 0 => random.shuffle(possibleEvents).head
      case _ => None
    }

  }
}
