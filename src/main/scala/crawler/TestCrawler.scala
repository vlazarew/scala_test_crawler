package crawler

import akka.actor.ActorSystem
import enums.{LogLevel, MessageType}
import helpers.{ArgsParser, MessageWriter}
import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization.{read, write}
import org.json4s.{DefaultFormats, JNothing}
import scalaj.http.Http
import sun.misc.{Signal, SignalHandler}

import java.io._
import java.nio.file.{Files, Path, Paths}
import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicBoolean
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

object TestCrawler extends App with SignalHandler {
  val name = "scala_test_crawler"

  val DEMO_URL = "https://demo-site.at.ispras.ru/"

  private val fileName = "outputFile.jsonl"

  val random = new Random

  lazy val outputFile: Path = {
    Files.deleteIfExists(Path.of(fileName))
    Files.createFile(Path.of(fileName))
  }

  val SIGINT = "INT"
  val SIGTERM = "TERM"
  val terminated = new AtomicBoolean(false)

  // регистрируем сам App объект, как обработчик сигналов
  Signal.handle(new Signal(SIGINT), this)
  Signal.handle(new Signal(SIGTERM), this)

  var metrics = Metrics()

  val startCrawlTime = Instant.now()

  val actorSystem = ActorSystem()
  val scheduler = actorSystem.scheduler
  implicit val executor: ExecutionContextExecutor = actorSystem.dispatcher

  //hook для akka, будет использоваться для любых остановок, не только по сигналам
  actorSystem.registerOnTermination {
    System.exit(0)
  }

  implicit val formats = DefaultFormats

  // собственно обработчик
  override def handle(signal: Signal): Unit = {
    if (terminated.compareAndSet(false, true) && List(SIGINT, SIGTERM).contains(signal.getName)) {
      MessageWriter.writeMessage(MessageType.Log, "CRAWLER SHUTDOWN")
      writeMetricsToFile(jobDirPathName)
      actorSystem.terminate()
    }
  }

  /** write metrics to file in JOBDIR */
  private def writeMetricsToFile(jobDirName: String): Unit = {
    if (jobDirPathName.nonEmpty) {
      val jobDirPath = Paths.get(jobDirName)
      val jobDir = if (!Files.exists(jobDirPath)) Files.createDirectory(jobDirPath) else jobDirPath
      val metricsJson = write(metrics)
      val metricsOutputFile = new PrintWriter(new File(s"$jobDir/saved_metrics.txt"))
      metricsOutputFile.write(metricsJson)
      metricsOutputFile.close()
    }
  }

  val metricsNotifier = scheduler.scheduleAtFixedRate(initialDelay = Duration.ofMinutes(1), interval = Duration.ofMinutes(1),
    runnable = () => {
      val metricsInfo = createMetricsBody(Instant.now())
      MessageWriter.writeMessage(MessageType.Stats, metricsInfo)
    }, executor)

  private val crawlerDefinition = typeOf[TestCrawler].members.withFilter(!_.isMethod)
    .map(el => (el.name.toString.trim, el.typeSignature)).toMap

  val crawlerArgs = ArgsParser.parse(args, crawlerDefinition)
  val jobDirPathName = crawlerArgs.getOrElse("JOBDIR", "").asInstanceOf[String]
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
      writeMetricsToFile(jobDirPathName)
    }

    metricsNotifier.cancel()
    System.exit(0)
  }

  private def getConfig(crawlerArgs: Map[String, Any]): TestCrawler = {
    TestCrawler(
      requests = crawlerArgs.getOrElse("requests", 1).asInstanceOf[Int],
      items = crawlerArgs.getOrElse("items", 100).asInstanceOf[Int],
      errors = crawlerArgs.getOrElse("errors", 0).asInstanceOf[Int],
      workTime = crawlerArgs.getOrElse("workTime", 60).asInstanceOf[Int],
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

    /** READ DATA FROM FILE IN JOBDIR */
    if (jobDirPathName.nonEmpty) {
      val filename = jobDirPathName + "/saved_metrics.txt"
      loadDataFromJobDir(filename)
    }
    MessageWriter.writeMessage(MessageType.Log, item)
  }

  /** READ DATA FROM FILE IN JOBDIR */
  private def loadDataFromJobDir(filename: String): Unit = {
    try {
      val bufferedSourceMetricsFile = Source.fromFile(filename)
      for (line <- bufferedSourceMetricsFile.getLines) {
        MessageWriter.writeMessage(MessageType.Log, s"Resuming crawl. Data loaded from $filename. Previous run metrics: $line")
        metrics = try {
          read[Metrics](line)
        }
        catch {
          case _: Throwable => metrics
        }
      }
      bufferedSourceMetricsFile.close()
    } catch {
      case _: FileNotFoundException => MessageWriter.writeMessage(MessageType.Log, s"Couldn't find file in JOBDIR=$jobDirPathName")
      case _: IOException => MessageWriter.writeMessage(MessageType.Log, "Got an IOException!")
    }
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
