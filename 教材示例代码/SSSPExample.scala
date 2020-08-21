import org.apache.log4j.{Level,Logger}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexId}
object SSSPExample {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //设置运行环境
    val conf = new SparkConf().setAppName("SSSPExample").setMaster("local")
    val sc = new SparkContext(conf)
    val inf = Double.PositiveInfinity
    // 初始化图，每个顶点表示用户，边表示用户各自的距离以及可达情况
    val users: RDD[(VertexId,String)] = 
	sc.parallelize(Array((1L,"wyn"), (2L,"hh"),(3L,"zyy"),(4L,"pj")))
    val distances: RDD[Edge[Double]] = 
	sc.parallelize(Array(Edge(1L,2L,2.0), Edge(1L,3L,0.5), Edge(2L,3L,2.0), Edge(2L,4L,1.0), Edge(3L,1L,1.0), Edge(4L,1L,2.0), Edge(4L,3L,0.5)))
    val defaultUser = "none"
    var graph = Graph(users, distances, defaultUser)
    // 设置源顶点是id为2的点
    val sourceId: VertexId = 2 
    // 初始化所有顶点的属性值为无穷，表示与源顶点的距离为无穷，而源顶点则为0
    var initialGraph = graph.mapVertices((vid,attr) =>
      if (vid == sourceId) 0.0 else inf)
    // 打印出原来图的信息
    println("原来图的顶点用户信息:")
    println(initialGraph.vertices.collect.mkString("\n"))
    println("原来图的边距离信息:")
    println(initialGraph.edges.collect.mkString("\n"))
    // Pregel API
    val sssp = initialGraph.pregel(inf)(
      // 顶点程序
      (id, dist, newDist) => math.min(dist, newDist),
      // 发送消息
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } 
        else {
          Iterator.empty
        }
      },
      // 组成消息
      (a, b) => math.min(a, b)
    )
    // 最终显示出(id,(name,dist))的格式信息
    val results = graph.vertices.join(sssp.vertices)
    println("迭代后的顶点信息:")
    println(results.collect.mkString("\n"))
  }
}
