#!/usr/bin/env bash
export MAVEN_OPTS="-Dorg.slf4j.simpleLogger.defaultLogLevel=error"
mvn clean package  > maven.log