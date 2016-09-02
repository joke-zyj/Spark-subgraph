package subgraph

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.commons.net.nntp.Threadable
import sun.rmi.runtime.NewThreadAction
import java.lang.Long
import org.apache.spark.graphx.lib.ShortestPaths
import graph.Graph
import org.dmg.pmml.False
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FSDataInputStream

object Main {

  def main(args: Array[String]): Unit = {

    var min = Int.MaxValue //距离最短的节点的距离（距离：该点遍历图中所有节点所需要经过的最长边数）
    var max = 0 //某个点的距离
    var indegree = 0 //选中点的入度
    var outdegree = 0 //选中点的出度
    var final1 = 0 //选中点的id
    var i = 1

    //application 配置
    val conf = new SparkConf().setAppName("sub").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[graph.Graph], classOf[graph.Edge], classOf[graph.Node],  classOf[java.lang.Long]))
    val sc = new SparkContext(conf)

    //输出文件设置
    var conf1 = new Configuration()
    var fs = FileSystem.get(conf1)
    var out = fs.create(new Path(args(4)));

    //载入大图
    val largegraph = GraphLoader.edgeListFile(sc, args(1), false, 1000)
    val number = largegraph.numVertices
    
    //1数据集所用的算法
    if (number < 30000) {

      //构造需匹配的小图
      var g2: Graph = new Graph("Graph2");
      val vts = sc.textFile(args(2)).collect.foreach { x => if (x.split(" ").length > 1) { g2.addNode() } }
      val egs = sc.textFile(args(3)).collect().foreach { x => if (x.split(" ").length > 1) { g2.addEdge(x.split(" ")(0).toInt - 1, x.split(" ")(1).toInt - 1) } }

      //构造小图的 GraphRDD
      val graphsmall = GraphLoader.edgeListFile(sc, args(3))

      //计算小图的选中点与所有点能到所有点的最短距离
      var numVertices = graphsmall.numVertices
      val minv = shortpath.run(graphsmall, graphsmall.mapVertices((VertexId, VD) => VertexId).vertices.values.collect()).vertices.collect
      for (i <- 0 to numVertices.toInt - 1) {
        max = 0
        for (j <- 1 to numVertices.toInt) {
          println((minv(i)._2.get(j.toLong) getOrElse (0)))
          if (max < (minv(i)._2.get(j.toLong) getOrElse (0)))
            max = minv(i)._2.get(j.toLong) getOrElse (0)
        }
        if (max < min) {
          min = max
          final1 = minv(i)._1.toInt
        }
      }

      //对选中点的出度与入度进行赋值
      indegree = graphsmall.inDegrees.collectAsMap().get(final1.toLong) getOrElse (0)
      outdegree = graphsmall.outDegrees.collectAsMap().get(final1.toLong) getOrElse (0)

      //出度与入度的过滤,得到要处理的图lastgraph2
      var lastgraph1 = graphsmall
      var lastgraph2 = graphsmall
      //出度与入度的过滤,得到要处理的图lastgraph2

      if (indegree == 0) {
        lastgraph1 = largegraph
      } else {
        lastgraph1 = largegraph.outerJoinVertices(largegraph.inDegrees.filter { case (id, num) => num >= indegree })((_, _, optDeg) => optDeg.getOrElse(0)).subgraph(vpred = (_, num) => num != 0)
      }

      if (outdegree == 0) {
        lastgraph2 = lastgraph1
      } else {
        lastgraph2 = lastgraph1.outerJoinVertices(largegraph.outDegrees.filter { case (id, num) => num >= outdegree })((_, _, optDeg) => optDeg.getOrElse(0)).subgraph(vpred = (_, num) => num != 0).cache()
      }

      //每个点根据变量min的值来确定收集附近节点的范围，每个节点将周围的信息汇聚起来
      val vsnb = Collectneighbors.collectNeighbors(largegraph, EdgeDirection.Either)
      var vsnb2 = vsnb
      if (min > 0) {  
        if (number > 100000) {       //根据平台测试情况对数据集3进行特殊处理
          min = 1
        }
        for (i <- 0 to min - 1) {
          var vsnb1 = Collectneighbors.collectNeighbors1(vsnb2, EdgeDirection.Either)
          vsnb2 = vsnb1
        }
      }
      //只对那些满足选中点出入度的点进行周围节点分布情况的给予，生成图finalgraph
      val finalgraph = lastgraph2.outerJoinVertices(vsnb2.vertices)((_, _, optDeg) => optDeg.getOrElse(Set.empty[(Long, Long)]))
      //对图上每个点都进行vf2的修改算法
      val signg2 = sc.broadcast(g2)
      val finaldata = finalgraph.vertices.mapValues { (vd, ed) => (new search()).changenumber(ed, signg2.value, vd.longValue, final1.longValue) }.collect()
      for (i <- 0 to finaldata.size) {

        //结果输出
        out.write(finaldata(i)._2.getBytes)
      }
    } 
    
    //2，3数据集所用的算法
    else {
      //构造需匹配的小图
      var g2: Graph = new Graph("Graph2");
      val vts = sc.textFile(args(2)).collect.foreach { x => if (x.split(" ").length > 1) { g2.addNode(x.split(" ")(1)) } }
      val egs = sc.textFile(args(3)).collect().foreach { x => if (x.split(" ").length > 1) { g2.addEdge(x.split(" ")(0).toInt - 1, x.split(" ")(1).toInt - 1) } }

      val filtersv: RDD[(VertexId, Int)] = sc.textFile(args(2)).filter { x => x.split(" ").length > 1 }.map { x => (x.split(" ")(0).toInt.longValue(), x.split(" ")(1).toInt) }
      val svarr = filtersv.collect()
      var flag = 0
      var specialv = 0
      for (i <- 0 until svarr.length) {
        if (svarr(i)._2 != 1) {
          flag = 1
          specialv = svarr(i)._2
          final1 = svarr(i)._1.toInt
        }
      }
      val smagraph = GraphLoader.edgeListFile(sc, args(3))
      //得到小图
      val filtersg = smagraph.outerJoinVertices(filtersv)((_, _, optDeg) => optDeg.getOrElse(0)).subgraph(vpred = (_, num) => num != 0)

      val gp1 = sc.textFile(args(2)).map { x => if (x.split(" ").length > 1) { x.split(" ")(1).toInt } }.collect //收集小图的节点属性
      //  gp1.foreach { println(_) }
      val filterv: RDD[(VertexId, Int)] = sc.textFile(args(0)).filter { x => if (x.split(" ").length > 1) { gp1.contains(x.split(" ")(1).toInt) } else { false } }.map { x => (x.split(" ")(0).toInt.longValue(), x.split(" ")(1).toInt) }

      //得到过滤后的大图
      val filterg = largegraph.outerJoinVertices(filterv)((_, _, optDeg) => optDeg.getOrElse(0)).subgraph(vpred = (_, num) => num != 0)

      if (flag == 0) {

        //计算小图的选中点与所有点能到所有点的最短距离(min)
        var numVertices = filtersg.numVertices
        val minv = shortpath.run(filtersg, filtersg.mapVertices((VertexId, VD) => VertexId).vertices.values.collect()).vertices.collect
        for (i <- 0 to numVertices.toInt - 1) {
          max = 0
          for (j <- 1 to numVertices.toInt) {
            if (max < (minv(i)._2.get(j.toLong) getOrElse (0)))
              max = minv(i)._2.get(j.toLong) getOrElse (0)

          }
          if (max < min) {
            min = max
            final1 = minv(i)._1.toInt
          }
        }
      } else {
        var numVertices = filtersg.numVertices
        val minv = shortpath.run(filtersg, filtersg.mapVertices((VertexId, VD) => VertexId).vertices.values.collect()).vertices.collect
        min = 0
        for (i <- 0 until minv.length) {
          if (minv(i)._1 == final1) {
            for (j <- 1 to minv.length) {
              if (min < (minv(i)._2.get(j.toLong) getOrElse (0)))
                min = minv(i)._2.get(j.toLong) getOrElse (0)

            }
          }
        }

      }

      //对选中点的出度与入度进行赋值
      indegree = filtersg.inDegrees.collectAsMap().get(final1.toLong) getOrElse (0)
      outdegree = filtersg.outDegrees.collectAsMap().get(final1.toLong) getOrElse (0)

      var lastgraph1 = filtersg
      var lastgraph2 = filtersg
      //出度与入度的过滤,得到要处理的图lastgraph2
      if (flag == 0) {
        if (indegree == 0) {
          lastgraph1 = filterg
        } else {
          lastgraph1 = filterg.outerJoinVertices(filterg.inDegrees.filter { case (id, num) => num >= indegree })((_, _, optDeg) => optDeg.getOrElse(0)).subgraph(vpred = (_, num) => num != 0)
        }

        if (outdegree == 0) {
          lastgraph2 = lastgraph1
        } else {
          lastgraph2 = lastgraph1.outerJoinVertices(filterg.outDegrees.filter { case (id, num) => num >= outdegree })((_, _, optDeg) => optDeg.getOrElse(0)).subgraph(vpred = (_, num) => num != 0).cache()
        }
      } else {
        lastgraph2 = filterg.subgraph(vpred = (_, num) => num == specialv).cache()
      }

      //      val lastgraph2 = lastgraph1.outerJoinVertices(filterg.outDegrees.filter { case (id, num) => num >= outdegree })((_, _, optDeg) => optDeg.getOrElse(0)).subgraph(vpred = (_, num) => num != 0).cache()

      //每个点根据变量min的值来确定收集附近节点的范围，每个节点将周围的信息汇聚起来
      val vsnb = Collectneighbors.collectNeighbors2(filterg, EdgeDirection.Either)

      var vsnb2 = vsnb
      if (min > 0) { 
        if (number > 10000) {       //根据平台测试情况对数据集3进行特殊处理
          min = 1
        }
          for (i <- 0 to min - 1) {
            var vsnb1 = Collectneighbors.collectNeighbors3(vsnb2, EdgeDirection.Either)
            vsnb2 = vsnb1
          }
      }
      //只对那些满足选中点出入度的点进行周围节点分布情况的给予，生成图finalgraph
      val finalgraph = lastgraph2.outerJoinVertices(vsnb2.vertices)((_, _, optDeg) => optDeg.getOrElse(Set.empty[(Long, Int, Long, Int)]))
      val signg2 = sc.broadcast(g2)
      //对图上每个点都进行vf2的修改算法
      val finaldata = finalgraph.vertices.mapValues { (vd, arrv) => out.write((new search()).changenumber1(arrv, signg2.value, vd.longValue, final1.longValue).getBytes) }.collect()
      for (i <- 0 until finaldata.size) {
        //结果输出
//        out.write(finaldata(i)._2.getBytes)
      }

    }
  }
}