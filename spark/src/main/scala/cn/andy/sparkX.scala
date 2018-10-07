package cn.andy

import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, sql}

/**
  * 使用spark的graphX来解决图数据库计算性能比较低的问题。
  */

class sparkX {

  val sc = new SparkContext()
  val hiveContext = new HiveContext(sc)
  val sqlDF: DataFrame = hiveContext.sql("select tar_pid,ori_pid from identify.p1_p2_rel_transfer_0911")
  private val custDF: DataFrame = hiveContext.sql("select cust_pid from identify.customer where is_deny =1")
  private val accDF: DataFrame = hiveContext.sql("select cust_pid from identify.account where is_deny =1")

  def md5ToLong(md5:String):Long ={

    var h = 0L
    for( i <- 0 until md5.length){
      h = 127 * h + md5.charAt(i)
    }

    h
  }

  val edgesRDDTMP: RDD[(VertexId, VertexId)] =
    sqlDF.rdd.map(row => (md5ToLong(DigestUtils.md5Hex(row.getAs[String]("ori_pid"))),md5ToLong(DigestUtils.md5Hex(row.getAs[String]("tar_pid")))))
    .union(custDF.rdd.map(row => (md5ToLong(DigestUtils.md5Hex("start-node")),md5ToLong(DigestUtils.md5Hex(row.getAs[String]("cust_pid"))))))
    .union(accDF.rdd.map(row => (md5ToLong(DigestUtils.md5Hex("start-node")),md5ToLong(DigestUtils.md5Hex(row.getAs[String]("cust_pid"))))))

  private val edgesRDD: RDD[(VertexId, VertexId)] = edgesRDDTMP.repartition(30)
  private val graph: Graph[Int, Int] = Graph.fromEdgeTuples(edgesRDD,1)

  private val root: VertexId = md5ToLong(DigestUtils.md5Hex("start-node"))
  private val initialGraph: Graph[Double, Int] = graph.mapVertices((id, _) => if ( id == root ) 0.0 else Double.PositiveInfinity)

  val vprog = {
    (id:VertexId ,attr:Double ,msg:Double)=>math.min(attr,msg)
  }

  private val sendMessage: EdgeTriplet[Double, Int] => Iterator[(VertexId, Double)] = {

    (triplet: EdgeTriplet[Double, Int]) =>
      var iterator: Iterator[(VertexId, Double)] = Iterator.empty
      val isSrcMarked = triplet.srcAttr != Double.PositiveInfinity
      val isDstMarked = triplet.dstAttr != Double.PositiveInfinity
      if (!(isSrcMarked && isDstMarked)) {
        if (isSrcMarked) {
          iterator = Iterator((triplet.dstId, triplet.srcAttr + 1))
        } else {
          iterator = Iterator((triplet.srcId, triplet.dstAttr + 1))
        }
      }

      iterator
  }

  private val reduceMessage: (Double, Double) => Double = {

    (a: Double, b: Double) => math.min(a, b)
  }
  reduceMessage

  val bfs = initialGraph.pregel(Double.PositiveInfinity,4)(vprog,sendMessage,reduceMessage)
  println(bfs.vertices.filter(_._2 == 4).collect().mkString("\n"))
}
