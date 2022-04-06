#!/bin/bash
command=$1

case $command in
"list")
  exec java -jar project.jar -a getCrawlerNames=true
  ;;
"schedule")
  exec java -jar project.jar ${@:2}
  ;;

esac
