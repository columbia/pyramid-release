package edu.columbia.cs.pyramid

import scala.collection.mutable

object Command extends Enumeration {
  type Command = Value
  val BUILD, TRANSFORM, DISCRETIZE, PREPARE, BUILD_HOT_WINDOW, TRANSFORM_HOT_WINOW = Value
}

object Dataset extends Enumeration {
  type Dataset = Value
  val CRITEO_KAGGLE, CRITEO_RAW, MOVIELENS_LATEST, VW_LABEL = Value
}

object NoiseType extends Enumeration {
  type NoiseType = Value
  val BACKINGNOISE, INITNOISE = Value
}

import Command._
import Dataset._
import NoiseType._

case class Config(
  command:  Command,
  dataset:  Dataset,
  featureCount: Int,
  labels: Array[Float],
  noiseType: NoiseType,
  noiseLaplaceB: Float,
  sumNNoiseDraws: Int,
  perFeatureNoiseLaplaceB: Array[Float],
  perFeatureCountTableInstanciation: Array[(String, Option[Double], Option[Double]) => CountTable]
)
