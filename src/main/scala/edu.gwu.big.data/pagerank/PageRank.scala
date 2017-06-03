package edu.gwu.big.data.pagerank

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Runs the PageRank algorithm on Spark
  */
object PageRank {
  val APP_NAME = "PageRank"

  def processLines(lines: RDD[String]): RDD[(String, String)] = {
    lines.flatMap(line => {
      val split = line.split("\\t")
      val listBuffer = ListBuffer.empty[(String, String)]

      for(i <- 2 until split.length) {
        val tuple = ( split(0), split(i) )
        listBuffer += tuple
      }

      listBuffer
    })
  }

  def generateLinks(pairs:RDD[(String, String)]): RDD[(String, Iterable[String])] = {
    pairs.distinct().groupByKey()
  }

  def generateRanks(links:RDD[(String, Iterable[String])]): RDD[(String, Double)] = {
    links.mapValues(v => 1.0)
  }

  def reduceRanks(links:RDD[(String, Iterable[String])], initialRanks: RDD[(String, Double)], iters: Int): RDD[(String, Double)] = {
    var ranks = initialRanks
    for(i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    ranks
  }

  def main(args: Array[String]): Unit = {
    val config = PageRankConfig.parse(args)

    val defaultConf = new SparkConf().setAppName(APP_NAME)

    val conf = if(config.runLocal) {
      defaultConf.setMaster("local[*]")
    }
    else {
      defaultConf
    }
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val lines = sc.textFile(config.inputPath)

    val processedLines = processLines(lines)
    val links = generateLinks(processedLines).cache()
    val ranks = generateRanks(links)
    val output = reduceRanks(links, ranks, config.numIterations).collect().sortBy{case (_, d) => d}
    output.foreach(tuple => println(s"${tuple._1} has rank: ${tuple._2}"))

    sc.stop()
  }
}