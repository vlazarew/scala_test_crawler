#!/bin/bash
command=$1

case $command in
"list")
  exec java -jar project.jar --crawlerNames
  ;;
"schedule")
  exec java -jar project.jar ${@:2}
  ;;

esac
