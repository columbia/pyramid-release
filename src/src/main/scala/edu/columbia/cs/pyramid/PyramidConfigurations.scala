package edu.columbia.cs.pyramid

import com.fasterxml.jackson.databind.JsonNode

/**
  * Traits to be used as an enumeration. This is a little weird but should
  * make pattern matching easier and I think will lead to nicer code overall.
  */
sealed trait ConfigurationType

case object IntConfiguration extends ConfigurationType

case object DoubleConfiguration extends ConfigurationType

case object StringConfiguration extends ConfigurationType

case object IntSeqConfiguration extends ConfigurationType

case object IntSeqSeqConfiguration extends ConfigurationType

case object DoubleSeqConfiguration extends ConfigurationType

case object BooleanConfiguration extends ConfigurationType

// Used for more complex embedded types to be parsed by the programmer.
case object JsonNodeConfiguration extends ConfigurationType

case object JsonNodeSeqConfiguration extends ConfigurationType

case class Configuration(val tag: String,
                         val required: Boolean,
                         val configurationType: ConfigurationType,
                         val defaultValue: Option[Any])

case class ConfigurationValue(val value: Any,
                              val configurationType: ConfigurationType)

class PyramidConfigurations(val configurations: Seq[Configuration],
                            val configurationValues: Map[String, ConfigurationValue]) {

  def this() {
    this(Seq[Configuration](), Map[String, ConfigurationValue]())
  }

  /**
    * Returns a boolean representing if the key exists in the table.
    *
    * @param tag
    * @return
    */
  def hasTag(tag: String): Boolean = {
    return configurationValues.contains(tag)
  }

  /**
    * Adds a new configuration and returns a new PyramidConfigurations set.
    *
    * @param configuration The configuration to add.
    * @return A new PyramidConfigurations set.
    */
  def addConfiguration(configuration: Configuration): PyramidConfigurations = {

    // TODO: refactor to make the configurations a map.
    if (configurations.map(_.tag).contains(configuration.tag)) {
      throw new UnsupportedOperationException(s"Already contains tag: ${configuration.tag}")
    }

    val newConfigurations: Seq[Configuration] =
      configurations :+ configuration
    return new PyramidConfigurations(newConfigurations, configurationValues)
  }

  /**
    * Adds a configuration that must be an integer.
    *
    * @param tag Tag used to access the
    * @param required
    * @return
    */
  def addIntConfiguration(tag: String,
                          required: Boolean,
                          defaultValue: Option[Int] = None): PyramidConfigurations = {
    val newConfiguration =
      Configuration(tag, required, IntConfiguration, defaultValue)
    return this.addConfiguration(newConfiguration)
  }

  def addDoubleConfiguration(tag: String,
                             required: Boolean,
                             defaultValue: Option[Double] = None): PyramidConfigurations = {
    val newConfiguration =
      Configuration(tag, required, DoubleConfiguration, defaultValue)
    return this.addConfiguration(newConfiguration)
  }

  def addStringConfiguration(tag: String,
                             required: Boolean,
                             defaultValue: Option[String] = None): PyramidConfigurations = {
    val newConfiguration =
      Configuration(tag, required, StringConfiguration, defaultValue)
    return this.addConfiguration(newConfiguration)
  }

  def addIntSeqConfiguration(tag: String,
                             required: Boolean,
                             defaultValue: Option[Seq[Int]] = None): PyramidConfigurations = {
    val newConfiguration =
      Configuration(tag, required, IntSeqConfiguration, defaultValue)
    return this.addConfiguration(newConfiguration)
  }

  def addIntSeqSeqConfiguration(tag: String,
                                required: Boolean,
                                defaultValue: Option[Seq[Seq[Int]]] = None): PyramidConfigurations = {
    val newConfiguration =
      Configuration(tag, required, IntSeqSeqConfiguration, defaultValue)
    return this.addConfiguration(newConfiguration)
  }

  def addDoubleSeqConfiguration(tag: String,
                                required: Boolean,
                                defaultValue: Option[Seq[Double]] = None): PyramidConfigurations = {
    val newConfiguration =
      Configuration(tag, required, DoubleSeqConfiguration, defaultValue)
    return this.addConfiguration(newConfiguration)
  }

  def addBoolConfiguration(tag: String,
                           required: Boolean,
                           defaultValue: Option[Boolean] = None): PyramidConfigurations = {

    val newConfiguration =
      Configuration(tag, required, BooleanConfiguration, defaultValue)
    return this.addConfiguration(newConfiguration)
  }

  def addJsonNodeConfiguration(tag: String,
                               required: Boolean,
                               defaultValue: Option[JsonNode] = None): PyramidConfigurations = {
    val newConfiguration =
      Configuration(tag, required, JsonNodeConfiguration, defaultValue)
    return this.addConfiguration(newConfiguration)
  }

  /**
    * Returns an integer matched to the
    *
    * @param tag
    * @return
    */
  def getIntConfiguration(tag: String): Int = {
    return getOptionIntConfiguration(tag) match {
      case Some(i) => i
      case None => throw new UnsupportedOperationException(f"Key not found $tag")
    }
  }

  def getOptionIntConfiguration(tag: String): Option[Int] = {
    return configurationValues.get(tag) match {
      case Some(configurationValue) => configurationValue match {
        case ConfigurationValue(v: Any, IntConfiguration) => Some(v.asInstanceOf[Int])
        case _ => throw new UnsupportedOperationException(f"$tag is not an IntConfiguration")
      }
      case None => None
    }
  }

  def getDoubleConfiguration(tag: String): Double = {
    return getOptionDoubleConfiguration(tag) match {
      case Some(d) => d
      case None => throw new UnsupportedOperationException(f"Key not found $tag")
    }
  }

  def getOptionDoubleConfiguration(tag: String): Option[Double] = {
    return configurationValues.get(tag) match {
      case Some(configurationValue) => configurationValue match {
        case ConfigurationValue(v: Any, DoubleConfiguration) => Some(v.asInstanceOf[Double])
        case _ => throw new UnsupportedOperationException(f"$tag is not an DoubleConfiguration")
      }
      case None => None
    }
  }

  def getStringConfiguration(tag: String): String = {
    return getOptionStringConfiguration(tag) match {
      case Some(s) => s
      case None => throw new UnsupportedOperationException(f"Key not found $tag")
    }
  }

  def getOptionStringConfiguration(tag: String): Option[String] = {
    return configurationValues.get(tag) match {
      case Some(configurationValue) => configurationValue match {
        case ConfigurationValue(v: Any, StringConfiguration) => Some(v.asInstanceOf[String])
        case _ => throw new UnsupportedOperationException(f"$tag is not an StringConfiguration")
      }
      case None => None
    }
  }

  def getIntSeqConfiguration(tag: String): Seq[Int] = {
    return getOptionIntSeqConfiguration(tag) match {
      case Some(s) => s
      case None => throw new UnsupportedOperationException(f"Key not found $tag")
    }
  }

  def getOptionIntSeqConfiguration(tag: String): Option[Seq[Int]] = {
    return configurationValues.get(tag) match {
      case Some(configurationValue) => configurationValue match {
        case ConfigurationValue(v: Any, IntSeqConfiguration) => Some(v.asInstanceOf[Seq[Int]])
        case _ => throw new UnsupportedOperationException(f"$tag is not an IntSeqConfiguration")
      }
      case None => None
    }
  }

  def getDoubleSeqConfiguration(tag: String): Seq[Double] = {
    return getOptionDoubleSeqConfiguration(tag) match {
      case Some(s) => s
      case None => throw new UnsupportedOperationException(f"Key not found $tag")
    }
  }

  def getOptionDoubleSeqConfiguration(tag: String): Option[Seq[Double]] = {
    return configurationValues.get(tag) match {
      case Some(configurationValue) => configurationValue match {
        case ConfigurationValue(v: Any, DoubleSeqConfiguration) => Some(v.asInstanceOf[Seq[Double]])
        case _ => throw new UnsupportedOperationException(f"$tag is not an IntSeqConfiguration")
      }
      case None => None
    }
  }

  def getIntSeqSeqConfiguration(tag: String): Seq[Seq[Int]] = {
    return getOptionIntSeqSeqConfiguration(tag) match {
      case Some(is) => is
      case None => throw new UnsupportedOperationException(f"Key not found $tag")
    }
  }

  def getOptionIntSeqSeqConfiguration(tag: String): Option[Seq[Seq[Int]]] = {
    return configurationValues.get(tag) match {
      case Some(configurationValue) => configurationValue match {
        case ConfigurationValue(v: Any, IntSeqSeqConfiguration) => Some(v.asInstanceOf[Seq[Seq[Int]]])
        case _ => throw new UnsupportedOperationException(f"$tag is not an IntSeqSeqConfiguration")
      }
      case None => None
    }
  }

  def getBoolConfiguration(tag: String): Boolean = {
    return getOptionBoolConfiguration(tag) match {
      case Some(b) => b
      case None => throw new UnsupportedOperationException(f"Key not found $tag")
    }
  }

  def getOptionBoolConfiguration(tag: String): Option[Boolean] = {
    return configurationValues.get(tag) match {
      case Some(configurationValue) => configurationValue match {
        case ConfigurationValue(v: Any, BooleanConfiguration) => Some(v.asInstanceOf[Boolean])
        case _ => throw new UnsupportedOperationException(f"$tag is not an BooleanConfiguration")
      }
      case None => None
    }
  }

  def getJsonNodeConfiguration(tag: String): JsonNode = {
    return getOptionJsonNodeConfiguration(tag) match {
      case Some(j) => j
      case None => throw new UnsupportedOperationException(f"Key not found $tag")
    }
  }

  def getOptionJsonNodeConfiguration(tag: String): Option[JsonNode] = {
    return configurationValues.get(tag) match {
      case Some(configurationValue) => configurationValue match {
        case ConfigurationValue(v: Any, JsonNodeConfiguration) => Some(v.asInstanceOf[JsonNode])
        case _ => throw new UnsupportedOperationException(f"$tag is not an BooleanConfiguration")
      }
      case None => None
    }
  }

  def getJsonNodeSeqConfiguration(tag: String): Seq[JsonNode] = {
    return getOptionJsonNodeSeqConfiguration(tag) match {
      case Some(j) => j
      case None => throw new UnsupportedOperationException(f"Key not found $tag")
    }
  }

  def getOptionJsonNodeSeqConfiguration(tag: String): Option[Seq[JsonNode]] = {
    return configurationValues.get(tag) match {
      case Some(configurationValue) => configurationValue match {
        case ConfigurationValue(v: Any, JsonNodeConfiguration) => Some(v.asInstanceOf[Seq[JsonNode]])
        case _ => throw new UnsupportedOperationException(f"$tag is not an BooleanConfiguration")
      }
      case None => None
    }
  }

  /**
    * Parses the JSON node for all added configurations and will throw an
    * exception if all mandatory configurations are not met.
    *
    * @param jsonNode The jsonNode representing the config file.
    */
  def parseConfigurations(jsonNode: JsonNode): PyramidConfigurations = {
    var configurationValueMap: Map[String, ConfigurationValue] = Map[String, ConfigurationValue]()
    configurations.foreach { configuration: Configuration =>
      parseConfiguration(jsonNode, configuration) match {
        case Some(configurationValue) =>
          configurationValueMap = configurationValueMap + (configuration.tag -> configurationValue)
        case None =>
      }
    }
    return new PyramidConfigurations(this.configurations, configurationValueMap)
  }

  def parseConfiguration(jsonNode: JsonNode, configuration: Configuration): Option[ConfigurationValue] = {
    jsonNode.get(configuration.tag) match {
      case tagNode: JsonNode => configuration.configurationType match {
        case IntConfiguration =>
          Some(ConfigurationValue(jsonNode.get(configuration.tag).asInt, IntConfiguration))
        case DoubleConfiguration =>
          Some(ConfigurationValue(jsonNode.get(configuration.tag).asDouble, DoubleConfiguration))
        case StringConfiguration =>
          Some(ConfigurationValue(jsonNode.get(configuration.tag).asText, StringConfiguration))
        case IntSeqConfiguration =>
          Some(ConfigurationValue(parseJsonSeq(jsonNode.get(configuration.tag)).map(_.asInt), IntSeqConfiguration))
        case IntSeqSeqConfiguration =>
          Some(ConfigurationValue(parseJsonSeq(jsonNode.get(configuration.tag)).map(x => parseJsonSeq(x).map(_.asInt)), IntSeqSeqConfiguration))
        case DoubleSeqConfiguration =>
            Some(ConfigurationValue(parseJsonSeq(jsonNode.get(configuration.tag)).map(_.asDouble), DoubleSeqConfiguration))
        case BooleanConfiguration =>
          Some(ConfigurationValue(jsonNode.get(configuration.tag).asBoolean, BooleanConfiguration))
        case JsonNodeConfiguration=>
          Some(ConfigurationValue(jsonNode.get(configuration.tag), JsonNodeConfiguration))
        case JsonNodeSeqConfiguration =>
          Some(ConfigurationValue(parseJsonSeq(jsonNode.get(configuration.tag)), JsonNodeSeqConfiguration))
      }
      case _ => configuration.required match {
        case true => throw new UnsupportedOperationException(s"Could not find required tag: ${configuration.tag}")
        case false => configuration.defaultValue match {
          case Some(v) => Some(ConfigurationValue(v, configuration.configurationType))
          case None => None
        }
      }
    }
  }

  def parseJsonSeq(jsonNode: JsonNode): Seq[JsonNode] = {
    require(jsonNode.isArray)
    var returnSeq = Seq[JsonNode]()
    var index = 0
    while (true) {
      if (jsonNode.get(index) == null) {
        return returnSeq
      }
      returnSeq = returnSeq :+ jsonNode.get(index)
      index += 1
    }
    return returnSeq
  }

}
