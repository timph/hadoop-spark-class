// NOTE: set the value srcDir to a string containing the absolute path of
// the location of the source data directory, lesson-5xx-resources!

import scala.util._
import org.apache.spark.sql._

val ctx: SQLContext = Try(sqlContext).getOrElse(new SQLContext(sc))
import ctx.implicits._

val rdd = sc.textFile(srcDir + "/tx.csv").map(_.split(",")).map(x => Tx(x(0), x(1), x(2).toDouble)).cache

case class Tx(date: String, desc: String, amount: Double)

val table = rdd.toDF()
table.printSchema
table.registerTempTable("tx")

println("Total # of rows:")
ctx.sql("SELECT COUNT(*) FROM tx").collect.foreach(println(_))

println("Big credits:")
println("  SQL:")
ctx.sql("SELECT * FROM tx WHERE amount > 0 ORDER BY amount DESC LIMIT 10").collect.foreach(println(_))
println("  DSL:")
table.filter("amount > 0").orderBy(table.col("amount").desc).limit(10).collect.foreach(println(_))

println("Big debits:")
println("  SQL:")
ctx.sql("SELECT * FROM tx WHERE amount < 0 ORDER BY amount LIMIT 10").collect.foreach(println(_))
println("  DSL:")
table.filter("amount < 0").orderBy(table.col("amount")).limit(10).collect.foreach(println(_))

val strlen = (s: String) => s.trim.length
ctx.udf.register("len", strlen)

ctx.sql("SELECT len(desc), desc FROM tx WHERE len(desc) >= 100 ORDER BY len(desc) DESC").collect.foreach(println(_))

val jt = ctx.read.json(srcDir + "/tx.jsons")
jt.registerTempTable("jtx")
jt.printSchema

ctx.sql("SELECT * FROM jtx WHERE amount > 0 ORDER BY amount DESC LIMIT 10").collect.foreach(println(_))
