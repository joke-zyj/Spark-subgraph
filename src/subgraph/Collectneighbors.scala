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
import org.apache.hadoop.fs.Path
import com.typesafe.config.impl.Path
import org.dmg.pmml.False
import scala.reflect.ClassTag
object Collectneighbors {
    def collectNeighbors[VD, ED: ClassTag](graph: Graph[VD, ED], edgeDirection: EdgeDirection): Graph[Set[(Long, Long)], ED] = {
    val nbrs = edgeDirection match {
      case EdgeDirection.Either =>
        graph.aggregateMessages[Set[(Long, Long)]](
          ctx => {
            ctx.sendToSrc(Set((ctx.srcId, ctx.dstId)))
            ctx.sendToDst(Set((ctx.srcId, ctx.dstId)))
          },
          (a, b) => a ++ b, TripletFields.All)
    }
    graph.outerJoinVertices(nbrs) { (vid, vdata, nbrsOpt) =>
      nbrsOpt.getOrElse(Set.empty[(Long, Long)])
    }
    
  
    }
    
    def collectNeighbors1[VD: ClassTag, ED: ClassTag](graph: Graph[Set[(Long, Long)], ED], edgeDirection: EdgeDirection): Graph[Set[(Long, Long)], ED] = {
    val nbrs1 = edgeDirection match {
      case EdgeDirection.Either =>
        graph.aggregateMessages[Set[(Long, Long)]](
          ctx => {
            ctx.sendToSrc(ctx.dstAttr)

            ctx.sendToDst(ctx.srcAttr)
          },
          (a, b) => a ++ b, TripletFields.All)
    }
     graph.outerJoinVertices(nbrs1) { (vid, vdata, nbrsOpt) =>
      nbrsOpt.getOrElse(Set.empty[(Long, Long)])
    }
    }
    
    
    def collectNeighbors2[VD , ED: ClassTag](graph: Graph[VD, ED], edgeDirection: EdgeDirection): Graph[Set[((Long, Int ,Long, Int))], ED] = {
    val nbrs = edgeDirection match {
      case EdgeDirection.Either =>
        graph.aggregateMessages[Set[(Long, Int, Long, Int)]](
          ctx => {
            ctx.sendToSrc(Set((ctx.srcId,ctx.srcAttr.toString().toInt,ctx.dstId,ctx.dstAttr.toString().toInt)))
            ctx.sendToDst(Set((ctx.srcId,ctx.srcAttr.toString().toInt, ctx.dstId,ctx.dstAttr.toString().toInt)))
          },
          (a, b) => a ++ b, TripletFields.All)
    }
    graph.outerJoinVertices(nbrs) { (vid, vdata, nbrsOpt) =>
      nbrsOpt.getOrElse(Set.empty[(Long, Int,Long, Int)])
    }
    
  
    }
    
    
    
    def collectNeighbors3[VD: ClassTag, ED: ClassTag](graph: Graph[Set[(Long, Int, Long, Int)], ED], edgeDirection: EdgeDirection): Graph[Set[(Long, Int, Long, Int)], ED] = {
    val nbrs1 = edgeDirection match {
      case EdgeDirection.Either =>
        graph.aggregateMessages[Set[(Long, Int, Long, Int)]](
          ctx => {
            ctx.sendToSrc(ctx.dstAttr)

            ctx.sendToDst(ctx.srcAttr)
          },
          (a, b) => a ++ b, TripletFields.All)
    }
     graph.outerJoinVertices(nbrs1) { (vid, vdata, nbrsOpt) =>
      nbrsOpt.getOrElse(Set.empty[(Long, Int, Long, Int)])
    }
    }
}