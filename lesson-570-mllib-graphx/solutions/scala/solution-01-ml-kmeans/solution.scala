
import org.apache.spark.ml._
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.feature._
import scala.math._

case class Tx(date: String, desc: String, amount: Double)
val training = sc.textFile(srcDir + "/tx.csv").map(_.split(",")).map(x => Tx(x(0), x(1), x(2).toDouble)).toDF.cache

val assembler = new VectorAssembler().setInputCols(Array("amount")).setOutputCol("features")
val k = 4
val maxIterations = 10000
val km = new KMeans().setMaxIter(maxIterations).setK(k)
val pl = new Pipeline().setStages(Array(assembler, km))

val model = pl.fit(training)

val centers = model.stages(1).asInstanceOf[KMeansModel].clusterCenters
println("Cluster Centers:")
centers.zipWithIndex.map(c => (c._2,c._1(0))).foreach(println(_))

case class Val(amount:Double)
val seq = List.range(0,4).map(pow(10, _)).flatMap(x => Array(Val(-5*x),Val(5*x)))
val test = sqlContext.createDataFrame(seq)

val result = model.transform(test)
println("Predictions:")
result.show()
