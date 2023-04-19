package helpers

import org.apache.commons.cli.{DefaultParser, Option, Options}

import scala.reflect.runtime

object ArgsParser {

  def parse(args: Seq[String], crawlerDefinition: Map[String, runtime.universe.Type]): Map[String, Any] = {
    val argsOption = Option.builder("a").hasArgs.build
    val settingsOption = Option.builder("s").hasArgs.build
    val options = new Options()
    options.addOption(argsOption)
    options.addOption(settingsOption)
    val parser = new DefaultParser
    val cmd = parser.parse(options, args.toArray)

    (for {
      options <- cmd.getOptions
      args <- options.getValues
    } yield args)
      .map(unparsed => unparsed.split("="))
      .filter(parsed => parsed.length == 2)
      .map(keyValue => {
        val key = keyValue(0)
        val value = keyValue(1)

        val possibleType = crawlerDefinition.get(key)
        val modifiedValue = possibleType match {
          case Some(fieldType) => try {
            fieldType.toString match {
              case "Int" => value.toInt
              case "Long" => value.toLong
              case "Boolean" => value.toBoolean
            }
          } catch {
            case _: Throwable => throw new Exception(s"field \"$key\" must be ${fieldType.toString} type and we cannot cast value \"$value\" to them.")
          }
          case None => value
        }

        key -> modifiedValue
      }).foldLeft(Map.empty[String, Any]) { case (map, value) => map + value }

  }
}
