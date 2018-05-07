// NOTE: set the value srcDir to a string containing the absolute path of
// the location of the source data directory, lesson-5xx-resources!

import scala.util._
import scala.math._
import org.apache.spark.sql._

val ctx: SQLContext = Try(sqlContext).getOrElse(new SQLContext(sc))
import ctx.implicits._

case class Tx(date: String, desc: String, amount: Double)

val rdd = sc.textFile(srcDir + "/tx.csv").map(_.split(",")).map(x => Tx(x(0), x(1), x(2).toDouble)).cache
