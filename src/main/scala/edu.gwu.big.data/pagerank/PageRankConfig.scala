package edu.gwu.big.data.pagerank

/**
  * Config class for the Spark job
  * @param runLocal Flag to run locally
  * @param numIterations Number of iterations to run for PageRank
  * @param inputPath The input path to read
  */
case class PageRankConfig(runLocal: Boolean = PageRankConfig.DEFAULT_BUILD_LOCAL,
                          numIterations: Int = PageRankConfig.DEFAULT_NUM_ITERATIONS,
                          inputPath: String = "") {

}

object PageRankConfig {
  val DEFAULT_BUILD_LOCAL = true
  val DEFAULT_NUM_ITERATIONS = 10

  def parse(args: Array[String]): PageRankConfig = {
    val parser = new scopt.OptionParser[PageRankConfig]("scopt") {
      head("scopt", "3.x")

      opt[Boolean]("runLocal").optional().action( (b, config) => config.copy(runLocal = b)).text(s"Whether to run page rank locally. Default is ${DEFAULT_BUILD_LOCAL}")

      opt[Int]("numIterations").optional().action( (i, config) => config.copy(numIterations = i) ).text(s"Number of iterations to run. Default is ${DEFAULT_NUM_ITERATIONS}")

      opt[String]("inputPath").required().action( (s, config) => config.copy(inputPath = s) ).text("Input path to read")
    }

    val config = parser.parse(args, PageRankConfig())
    if(config.isEmpty){
      System.exit(-1)
    }
    config.get
  }
}