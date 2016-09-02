package subgraph

import graph.Graph
import matcher.VF2Matcher
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import java.io.IOException
import java.io.OutputStream
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import java.lang.Long
import org.apache.hadoop.fs.Path;

import java.util.ArrayList
import java.util.Iterator
import java.lang.Long

object search extends serializable {

}
class search extends serializable {

  def changenumber(vset: Set[(Long, Long)], g2: Graph, initid: Long, initm: Long): String = {

    var ar = new ArrayList
    var matcher = new VF2Matcher //创建算法对象
    var lastv = Array(0)  //最终传输给算法的序列
    var initv = vset.toArray
    var ii = 0
    val ary = vset.toArray
    var mapdata: String = ""
    var unrepv = initv.flatMap(cg => Array(cg._1) ++ Array(cg._2)).toSet.toArray

    //指定大图匹配点
    var initn = unrepv.lastIndexOf(initid)
    for (i <- 0 until unrepv.size) {
      if (i != 0) {
        lastv = lastv ++ Array(i)
      }
    }
    
    //由该点周围点构成的大图
    var g1: Graph = new Graph("Graph1");
    for (i <- 0 until lastv.size) {
      g1.addNode()
    }
    for (i <- 0 until initv.size) {
      g1.addEdge(unrepv.indexOf(initv(i)._1), unrepv.indexOf(initv(i)._2))
    }

    //进行匹配算法，传入参数大图，小图，指定大图与小图的匹配点
    var result = matcher.find(g1, g2, initn, initm - 1)
      
    //对匹配结果进行处理,得到结果字符串用于函数返回
    for (i <- 0 until result.size()) {
      for (ii <- 0 until result.get(i).size) {
        if (ii != result.get(i).size - 1) {
          mapdata = (mapdata + unrepv((result.get(i)).get(ii)) + ",")
        } else {
          mapdata = (mapdata + unrepv((result.get(i)).get(ii)) + "\n")
        }
      }
    }
    mapdata
  }
  
  
    def changenumber1(vset: Set[(Long, Int, Long, Int)], g2: Graph, initid: Long, initm: Long): String = {

    var ar = new ArrayList
    var matcher = new VF2Matcher //创建算法对象
   
    var initv = vset.toArray
    var ii = 0
    val ary = vset.toArray
    var mapdata: String = ""
    var unrepv = (initv.flatMap(cg => Array((cg._1, cg._2)) ++ Array((cg._3, cg._4)))).toSet.toArray
    
    var lastv = Array((0,0))
    if (unrepv.size > 0) {
      lastv = Array((0,unrepv(0)._2))  //最终传输给算法的序列
    }else {
      return ""
    }
    
     
    //指定大图匹配点
     var initn = 0
     for(i <- 0 until unrepv.length)
       if (unrepv(i)._1 == initid) {
         initn = unrepv.lastIndexOf((initid, unrepv(i)._2))
       }
    for (i <- 0 until unrepv.size) {
      if (i != 0) {
        lastv = lastv ++ Array((i, unrepv(i)._2))
      }
    }
    
    //由该点周围点构成的大图
    var g1: Graph = new Graph("Graph1");
    for (i <- 0 until lastv.size) {
      g1.addNode(lastv(i)._2.toString())
    }
    for (i <- 0 until initv.size) {
      g1.addEdge(unrepv.indexOf((initv(i)._1,initv(i)._2)), unrepv.indexOf((initv(i)._3,initv(i)._4)))
    }

    //进行匹配算法，传入参数大图，小图，指定大图与小图的匹配点
    var result = matcher.find(g1, g2, initn, initm - 1)
      
    //对匹配结果进行处理,得到结果字符串用于函数返回
    for (i <- 0 until result.size()) {
      for (ii <- 0 until result.get(i).size) {
        if (ii != result.get(i).size - 1) {
          mapdata = (mapdata + (unrepv((result.get(i)).get(ii)))._1 + ",")
        } else {
          mapdata = (mapdata + (unrepv((result.get(i)).get(ii)))._1 + "\n")
        }
      }
    }
    mapdata
  }

}