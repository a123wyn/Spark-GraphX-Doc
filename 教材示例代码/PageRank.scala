import org.apache.log4j.{Level,Logger}
import org.apache.spark._
import org.apache.spark.graphx.GraphLoader
object PageRank {
  def main(args: Array[String]) {
    // 屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    // 设置运行环境
    val conf = new SparkConf().setAppName("PageRank").setMaster("local")
    val sc = new SparkContext(conf)
    // 读取followers.txt
    val graph = GraphLoader.edgeListFile(sc, "file:///usr/local/spark/data/graphx/followers.txt")
    // 运行PageRank
    val ranks = graph.pageRank(0.0001).vertices
    // 将排名与用户名联系在一起
    val users = sc.textFile("file:///usr/local/spark/data/graphx/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // 打印结果
    println(ranksByUsername.collect().mkString("\n"))
  }
}
