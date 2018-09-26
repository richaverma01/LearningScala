// Databricks notebook source
val defaultMoviesUrl = "https://iomegastorage.blob.core.windows.net/data/movies.csv"
val defaultRatingsUrl = "adl://iomegadatalakestore.azuredatalakestore.net/data/ratings.csv"

val moviesUrl = dbutils.widgets.text("moviesUrl","")
val ratingsUrl = dbutils.widgets.text("ratingsUrl", "")

var inputMoviesUrl = dbutils.widgets.get("moviesUrl")

if(inputMoviesUrl == null) {
  inputMoviesUrl = defaultMoviesUrl
}

var inputRatingsUrl = dbutils.widgets.get("ratingsUrl")

if(inputRatingsUrl == null) {
  inputRatingsUrl = defaultRatingsUrl
}

// COMMAND ----------

package com.microsoft.analytics.utils

import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction

object MovieUtils {
  def loadMovieNames(fileName: String): Map[Int, String] = {
    if(fileName == null || fileName == "") {
      throw new Exception("Invalid File / Reference URL Specified!");
    }

    implicit val codec = Codec("UTF-8") //encoding of files passed as input to the program

    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val lines = Source.fromURL(fileName).getLines

    lines.drop(1)

    var movieNames: Map[Int, String] = Map()

    for(line <- lines) {
      val records = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
      val movieId = records(0).toInt
      val movieName = records(1)

      movieNames += (movieId -> movieName)
    }

    movieNames
  }

}

// COMMAND ----------

import com.microsoft.analytics.utils._

val broadcastedMovies = sc.broadcast(() => { MovieUtils.loadMovieNames(inputMoviesUrl)}) 

// COMMAND ----------

spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "75da8ba9-228d-49af-bbba-e738e0695a84")
spark.conf.set("dfs.adls.oauth2.credential", "HulA10r5XYIJJi4zYDkPoTBMXqLYNml7L+cwuuj4YqM=")
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/97ea9543-5331-4483-aafd-b4673f025370/oauth2/token")

// COMMAND ----------

spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.access.token.provider.type", spark.conf.get("dfs.adls.oauth2.access.token.provider.type"))
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.client.id", spark.conf.get("dfs.adls.oauth2.client.id"))

spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.credential", spark.conf.get("dfs.adls.oauth2.credential"))

spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.refresh.url", spark.conf.get("dfs.adls.oauth2.refresh.url"))

val ratingsData = sc.textFile(inputRatingsUrl)
val originalData = ratingsData.mapPartitionsWithIndex((index, iterator) => {
if(index == 0) iterator.drop(1)

 else iterator
})
val mappedData = originalData.map(line => { val splitted = line.split(",")

(splitted(1).toInt, 1)
})
val reducedData = mappedData.reduceByKey((x, y) => (x + y))
val result = reducedData.sortBy(_._2).collect
val finalOutput = result.reverse.take(10)
val mappedFinalOuptut = finalOutput.map(record => (broadcastedMovies.value()(record._1), record._2))

// COMMAND ----------

finalOutput.take(10).foreach(println)

// COMMAND ----------

mappedFinalOuptut.take(10).foreach(println)

// COMMAND ----------

