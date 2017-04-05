package edu.gwu.data.pagerank

import com.holdenkarau.spark.testing.SharedSparkContext
import edu.gwu.big.data.pagerank.PageRank
import edu.gwu.big.data.pagerank.PageRank.{generateLinks, generateRanks, processLines, reduceRanks}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class PageRankTest extends FunSuite with SharedSparkContext {
  test("Parse small.txt") {
    val inputStream = this.getClass.getResourceAsStream("/small.txt")
    val lines = Source.fromInputStream(inputStream).getLines()
    val rddLines = sc.parallelize(lines.toSeq)
    val processedLines = PageRank.processLines(rddLines)
    assert(!processedLines.isEmpty())
    assert(156 == processedLines.count())

    val links = generateLinks(processedLines).cache()
    val ranks = generateRanks(links)
    val output = reduceRanks(links, ranks, 10).collect().sortBy{case (_, d) => -1 * d}

    output.foreach(tuple => println(s"${tuple._1}\t${tuple._2}"))
  }

}
