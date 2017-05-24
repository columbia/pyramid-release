package edu.columbia.cs.pyramid

object PrepareVWLabel {

  val wsReg = "\\s+".r

  val barRegex = "\\|\\S*".r

  def isNumber(str: String): Boolean = {
    try {
      str.trim.toFloat
      return true
    } catch {
      case e: java.lang.NumberFormatException => {
        return false
      }
    }
  }

  def labelFeatures(features: Seq[String]): Seq[String] = {
    val labeledFeatures = features.zipWithIndex.map {
      case(str: String, index: Int) =>
        if ((index < 14 && isNumber(str)) || (str == "0")) {
          "f" + index + ":" + str
        } else {
          str
        }
    }
    return labeledFeatures
  }

  def labelLineFeatures(line: String): String = {
    val labelSplit = barRegex.split(line.trim)
    require(labelSplit.length == 2)
    val labledFeatures = labelFeatures(wsReg.split(labelSplit(1)))
    return labelSplit(0) + " |f " + labledFeatures.mkString(" ")
  }

  def executePrepareCommand(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Expected args <input file> <output file>")
    }
    val bufferSize = 10000
    val inputPath = args(0)
    val outputPath = args(1)
    val outputStream = CountTableLib.getOutputStream(outputPath)
    val inputStream = CountTableLib.getInputStream(inputPath)
    inputStream.getLines().grouped(bufferSize).foreach{ lines =>
      val labeledLines = lines.par.map{ line: String =>
        labelLineFeatures(line)
      }
      labeledLines.toList.map{ labeledLine =>
        outputStream.write(labeledLine)
        outputStream.write("\n")
      }
    }
    outputStream.close()
    inputStream.close()
  }
}
