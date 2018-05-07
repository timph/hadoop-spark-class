import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

case class Transfer(src:Long,dst:Long,amount:Double)

val tx = sc.textFile(srcDir + "/txfr.csv").distinct.map(_.split(",")).map(x => Transfer(x(0).toLong,x(1).toLong,x(2).toDouble)).cache
println("Tx count: " + tx.count)
val accts = tx.flatMap(x => Array(x.src,x.dst)).distinct
val vertices = accts.map(x => (x, x))
val edges = tx.map(x => Edge(x.src, x.dst, x.amount))

val graph = Graph(vertices, edges)

val top5in = graph.inDegrees.sortBy(_._2, false).take(5)
println("Top 5 receivers:")
top5in.foreach(println(_))

val top5out = graph.outDegrees.sortBy(_._2, false).take(5)
println("Top 5 senders:")
top5out.foreach(println(_))

val top5inout = graph.degrees.sortBy(_._2, false).take(5)
println("Top 5 most active:")
top5inout.foreach(println(_))

val ranks = graph.pageRank(0.0001).vertices
println("Top 5 by PageRank:")
ranks.sortBy(_._2, ascending = false).take(5).foreach(println(_))

val tx2 = sc.textFile(srcDir + "/txfr-with-bias.csv").distinct.map(_.split(",")).map(x => Transfer(x(0).toLong,x(1).toLong,x(2).toDouble)).cache
println("Tx2 count: " + tx2.count)
val accts2 = tx2.flatMap(x => Array(x.src,x.dst)).distinct
val vertices2 = accts2.map(x => (x, x))
val edges2 = tx2.filter(_.amount > 100).map(x => Edge(x.src, x.dst, x.amount))

val graph2 = Graph(vertices2, edges2)

val top5in2 = graph2.inDegrees.sortBy(_._2, false).take(5)
println("Top 5 receivers:")
top5in2.foreach(println(_))

val top5out2 = graph2.outDegrees.sortBy(_._2, false).take(5)
println("Top 5 senders:")
top5out2.foreach(println(_))

val top5inout2 = graph2.degrees.sortBy(_._2, false).take(5)
println("Top 5 most active:")
top5inout2.foreach(println(_))

val ranks2 = graph2.pageRank(0.0001).vertices
println("Top 5 by PageRank:")
ranks2.sortBy(_._2, ascending = false).take(5).foreach(println(_))
