package edu.columbia.cs.pyramid

import com.fasterxml.jackson.databind.JsonNode

class PrepareCriteoKaggleOperation extends PyramidOperation {

  override val operationName: String = "prepareCriteoKaggleOperation"

  /**
    * Execute the associated operation with the configuration.
    */
  override def execute(jsonNode: JsonNode): Unit = {}

  override val pyramidConfigs: PyramidConfigurations = new PyramidConfigurations()
}