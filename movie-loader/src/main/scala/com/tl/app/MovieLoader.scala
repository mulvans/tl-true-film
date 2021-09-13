package com.tl.app

import com.tl.app.AppConfig.RunInformation
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, desc, regexp_replace, year}

import java.util.Properties

class MovieLoader(spark: SparkSession, runInformation: RunInformation) {
  val RATIO_COLUMN_NAME = "ratio"
  val REVENUE_COLUMN_NAME = "revenue"
  val BUDGET_COLUMN_NAME = "budget"
  val TITLE_COLUMN_NAME = "title"
  val ABSTRACT_COLUMN_NAME = "abstract"
  val WIKI_ABSTRACT_COLUMN_NAME = "wikipedia_" + ABSTRACT_COLUMN_NAME
  val PRODUCTION_COMPANIES_COLUMN_NAME = "production_companies"
  val RATING_COLUMN_NAME = "rating"
  val WIKI_LINK_COLUMN_NAME = "wikipedia_link"
  val REPLACE_PATTERN = "Wikipedia: "
  val YEAR_COLUMN_NAME = "year"
  val RELEASE_DATE_COLUMN_NAME = "release_date"
  val PRODUCTION_COMPANY_COLUMN_NAME = "production_company"
  val VOTE_AVERAGE_COLUMN_NAME = "vote_average"
  val URL_COLUMN_NAME = "url"
  val USER = "user"
  val PASSWORD = "password"
import spark.implicits._

  def runJob(): Unit = {

    val metaData: DataFrame = spark.read.option("header", true)
      .csv(runInformation.movieMetaDataPath)
      .withColumn(RATIO_COLUMN_NAME, col(REVENUE_COLUMN_NAME) / col(BUDGET_COLUMN_NAME))
      .withColumn(YEAR_COLUMN_NAME, year(col(RELEASE_DATE_COLUMN_NAME)))
      .withColumnRenamed(PRODUCTION_COMPANIES_COLUMN_NAME, PRODUCTION_COMPANY_COLUMN_NAME)
      .withColumnRenamed(VOTE_AVERAGE_COLUMN_NAME, RATING_COLUMN_NAME)

    //cache to avoid re-read
    metaData.cache()

    //read in wikipedia data
    val wikipediaData: DataFrame = spark.read.format("com.databricks.spark.xml").schema(Schemas.wikipediaDataSchema)
      .option("rowTag", "doc")
      .load(runInformation.wikipediaDataPath)
      .withColumn(TITLE_COLUMN_NAME, regexp_replace(col(TITLE_COLUMN_NAME), REPLACE_PATTERN, ""))
      .withColumnRenamed(ABSTRACT_COLUMN_NAME, WIKI_ABSTRACT_COLUMN_NAME)
      .withColumnRenamed(URL_COLUMN_NAME, WIKI_LINK_COLUMN_NAME)

    //columns that will be selected for output
    val selectedColumns: List[String] = List(TITLE_COLUMN_NAME, BUDGET_COLUMN_NAME, YEAR_COLUMN_NAME, REVENUE_COLUMN_NAME,
      RATING_COLUMN_NAME, RATIO_COLUMN_NAME, PRODUCTION_COMPANY_COLUMN_NAME,
      WIKI_LINK_COLUMN_NAME, WIKI_ABSTRACT_COLUMN_NAME)

    //broadcast join faster, as metadata is small
    val filmDataOutput: DataFrame = wikipediaData.join(broadcast(metaData), Seq(TITLE_COLUMN_NAME)).select(selectedColumns.map(col): _*)
      .orderBy(desc(RATIO_COLUMN_NAME))
      .limit(runInformation.topAmount)

    //output with option to output to file
    if (runInformation.outputToFile) {
      filmDataOutput.write.csv(runInformation.outputPath)
    } else {
      filmDataOutput.write.mode(SaveMode.Overwrite).option("driver", "org.postgresql.Driver").jdbc(runInformation.dbUrl, runInformation.tableName,
        createDBProperties(runInformation.dbUser, runInformation.dbPassword))
    }

  }
   def createDBProperties(user: String, password: String): Properties = {
     val connectionProperties: Properties = new Properties()
     connectionProperties.put(USER, user)
     connectionProperties.put(PASSWORD, password)
     connectionProperties
   }
}
object MovieLoader extends App {

  private val runInformation: RunInformation = AppConfig.loadRunInformation

  private val spark = SparkSession.builder()
    .appName("truefilm movie loader")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .master("local[*]")
    .getOrCreate()

  private val movieLoader: MovieLoader = new MovieLoader(spark, runInformation)
  movieLoader.runJob()
}
