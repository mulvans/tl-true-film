import com.tl.app.AppConfig.RunInformation
import com.tl.app.MovieLoader
import com.tl.app.MovieLoader.spark
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import scala.reflect.io.Directory

class MovileLoaderTest extends AnyFlatSpec with BeforeAndAfter{

  after {
    val dir = new Directory(new File("movie-loader/target/output"))
    dir.deleteRecursively()
  }

  "movie loader" should "return top movies according to ratio" in {
    //build sparkSession
    val spark = SparkSession.builder()
      .appName("truefilm movie loader")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()

    //build runInformation object
    val runInformation = RunInformation(
      "jdbc:postgresql://localhost:5432/test_db",
      "top_movies",
      "movie-loader/src/test/scala/resources/movie_metadata.csv",
      "movie-loader/src/test/scala/resources/wikiDump.xml",
      outputToFile = true,
      topAmount = 100,
      outputPath = "movie-loader/target/output")
    val movieLoader = new MovieLoader(spark, runInformation)
    movieLoader.runJob()

    val sparkVerifyOutput = SparkSession.builder()
      .appName("truefilm movie loader")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()

    //verify output matches expected
    val expectedOutput = sparkVerifyOutput.read.csv("movie-loader/src/test/scala/resources/expectedOutput.csv")
    val actualOutput = sparkVerifyOutput.read.csv("movie-loader/target/output/")

    assert(expectedOutput.union(actualOutput).distinct().count() == expectedOutput.intersect(actualOutput).count())
  }

}
