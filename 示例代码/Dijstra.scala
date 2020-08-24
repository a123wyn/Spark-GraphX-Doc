import org.apache.log4j.{Level,Logger}

import org.apache.spark._

import org.apache.spark.graphx._

import org.apache.spark.rdd.RDD

import org.apache.spark.graphx.{Graph, VertexId}

object Dijstra {

  def main(args: Array[String]) {

    // 屏蔽日志

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // 设置运行环境

    val conf = new SparkConf().setAppName("Dijstra").setMaster("local")

    val sc = new SparkContext(conf)

    val inf = Double.PositiveInfinity

    // 初始化图，每个顶点表示用户，边表示用户各自的距离以及可达情况

    val users: RDD[(VertexId,String)] = 

	sc.parallelize(Array((1L,"wyn"), (2L,"hh"),(3L,"zyy"),(4L,"pj")))

    val distances: RDD[Edge[Double]] = 

	sc.parallelize(Array(Edge(1L,2L,2.0), Edge(1L,3L,0.5), Edge(2L,3L,2.0), Edge(2L,4L,1.0), Edge(3L,1L,1.0), Edge(4L,1L,2.0), Edge(4L,3L,0.5)))

    val defaultUser = "none"

    var graph = Graph(users, distances, defaultUser)

    // 设置源顶点是id为2的点，即用户hh负责通知其他用户

    val sourceId: VertexId = 2

    // 初始化所有顶点，顶点属性值格式为(dist,0/1)，其中dist表示源顶点到各顶点的距离，初始值为无穷，表示与源顶点的距离为无穷，而源顶点本身则为0;第二项0/1，为0表示顶点还未访问，为1表示顶点已访问

    var initialGraph = graph.mapVertices((vid,attr) =>

      (if (vid == sourceId) 0.0 else inf, false))

    println("原来图的顶点用户信息:")

    println(initialGraph.vertices.collect.mkString("\n"))

    println("原来图的边距离信息:")

    println(initialGraph.edges.collect.mkString("\n"))

    // 自定义一个求较小值的函数，用于选取当前拥有最短路径的顶点

    def min(a: (VertexId,(Double,Boolean)), b: (VertexId,(Double,Boolean))): (VertexId,(Double,Boolean)) = {

  	if (a._2._1 < b._2._1) a else b

    }

    // 开始迭代计算，每轮遍历图中所有未被访问的顶点，将源顶点出发到达的最短路径的目的顶点作为当前顶点，开始对该顶点的相邻顶点进去路径更新

    for (i <- 1L to initialGraph.vertices.count){

    // 在这里将未访问过的顶点过滤出来从中选出拥有最短路径的顶点

	val currentid = initialGraph.vertices.filter{ case (vid,attr) => !attr._2}.reduce(min)._1

	val newdist = initialGraph.aggregateMessages[Double](

	    // sendMsg: 向当前用户的相邻用户发送消息，内容为到相邻边的距离与最短路径值之和

	    triplet => {

	        if (triplet.srcId == currentid)

		    triplet.sendToDst(triplet.srcAttr._1 + triplet.attr)

	    },

	    // mergeMsg: 选择较小的值为源用户到该用户的最短路径值，实际上就是最短路径的更新

	    (a,b) => math.min(a,b))

    // 一轮消息发送完后需要执行outerJoinVertices对原来的顶点进行更新，只会对上一轮接受到消息的顶点进行更新，并且将当前顶点设置成已访问状态

	initialGraph = initialGraph.outerJoinVertices(newdist)((vid, attr, newdist) =>

		(math.min(attr._1, newdist.getOrElse(inf)), attr._2 || vid == currentid))

    }

    // 最终显示出(id,(name,dist))的格式信息

    val results = graph.vertices.join(initialGraph.vertices).map{

	case (vid, (username, attr)) => (vid, (username, attr._1))

    }

    // 打印最终信息

    println("迭代后的顶点信息:")

    println(results.collect.mkString("\n"))

  }

}
