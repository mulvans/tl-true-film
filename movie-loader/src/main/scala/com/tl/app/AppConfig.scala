package com.tl.app

import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader.arbitraryTypeValueReader

import java.util.logging.{Level, Logger}
import scala.util.{Failure, Success, Try}

object AppConfig {
  //object to load in config from spark-submit args
  private val MOVIE_LOADER_KEY = "ml"
  private val LOGGER = Logger.getLogger(getClass.getName)

  def loadRunInformation: RunInformation = {
    Try(ConfigFactory.load.as[RunInformation](MOVIE_LOADER_KEY)) match {
      case Success(config) => config
      case Failure(exception) =>
        LOGGER.log(Level.INFO, s"Encountered error when parsing application args ${exception.getMessage}")
        throw exception
    }
  }

  case class RunInformation(dbUrl: String, tableName: String,
                            movieMetaDataPath: String, wikipediaDataPath: String,
                            outputToFile: Boolean = false, topAmount: Int, dbUser: String = "root",
                            dbPassword: String = "root", outputPath: String = "")
}
