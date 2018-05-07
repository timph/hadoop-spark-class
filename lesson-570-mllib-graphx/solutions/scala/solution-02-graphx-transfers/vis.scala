// NOTE:  You can run this file, but you need to make sure spark-shell is invoked
// with the GraphStream jars on its classpath!
// For example:
// $SPARK_HOME/bin/spark-shell --master 'local[*]' --jars $(find '../lib/*.jar' | xargs echo | tr ' ' ',')
//
// To download those jars, use the script get-graphstream.sh in the ../lib directory!
//
// Also, don't forget to set val srcDir -- it's used below in the sc.textFile call!

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import scala.math._

import org.graphstream.graph.{Graph => GraphStream}
import org.graphstream.graph.implementations._

val thresholdAmount = 990.00
val filename = "txfr.csv"

case class Transfer(src:Long,dst:Long,amount:Double)

val tx = sc.textFile(srcDir + "/" + filename).distinct.map(_.split(",")).map(x => Transfer(x(0).toLong,x(1).toLong,x(2).toDouble)).filter(_.amount > thresholdAmount).cache
println("Tx count: " + tx.count)
println("Tx max: " + tx.sortBy(_.amount, false).first)

val accts = tx.flatMap(x => Array(x.src,x.dst)).distinct
val vertices = accts.map(x => (x, x))
val edges = tx.map(x => Edge(x.src, x.dst, x.amount))

val graph = Graph(vertices, edges)

val sg: SingleGraph = new SingleGraph("transfers")
sg.addAttribute("ui.stylesheet","url(file:" + srcDir + "/stylesheet)")
sg.addAttribute("ui.quality")
sg.addAttribute("ui.antialias")

for ((id,_) <- graph.vertices.collect()) {
  val node = sg.addNode(id.toString).asInstanceOf[SingleNode]
}
for (Edge(x,y,z) <- graph.edges.collect()) {
  try {
    val edge = sg.addEdge(x.toString ++ "_" ++ y.toString ++ "_" ++ (z*100).toLong.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
  }
  catch {
    case x: Throwable => println(x.getMessage)
  }
}

sg.display();
