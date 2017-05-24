package edu.columbia.cs.pyramid

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{ObjectMapper, JsonNode}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
  * Class abstracting pyramid operations.
  */
abstract class PyramidOperation {

  val operationName: String

  val pyramidConfigs: PyramidConfigurations

  private def getObjectMapper(): ObjectMapper = {
    val objMapper = new ObjectMapper()
    objMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true)
    objMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
    objMapper.registerModule(DefaultScalaModule)
    return objMapper
  }

  private def readConfigFile(configPath: String): JsonNode = {
    val source = scala.io.Source.fromFile(configPath)
    val objectMapper = getObjectMapper()
    val configString = source.getLines().mkString("\n")
    return objectMapper.readTree(configString)
  }

  final def execute(configPath: String): Unit = {
    val jsonNode: JsonNode = readConfigFile(configPath)
    execute(jsonNode)
  }

  /**
    * Execute the associated operation with the configuration.
    */
  def execute(jsonNode: JsonNode): Unit
}
