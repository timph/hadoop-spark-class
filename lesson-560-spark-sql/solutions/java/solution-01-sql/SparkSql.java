package com.scispike.spark;

iimport java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;


public class SparkSql implements Serializable {

    private static final long serialVersionUID = 1l;

	private void runJob() {

		String srcDir = "/Users/kmusial/hadoopupdate/data";
		String file = srcDir + "/tx.csv";

	    SparkSession spark = SparkSession.builder().appName("SparkSql").getOrCreate();
	    spark.sparkContext().setLogLevel("ERROR");

	    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

	    JavaRDD<String> lines = spark.read().textFile(file).javaRDD();

	    JavaRDD<Transaction> txns = lines.map(s -> s.split(",")).map(s -> new Transaction(s[0], s[1], Double.parseDouble(s[2])));

	    Dataset<Row> txnsDF = spark.createDataFrame(txns, Transaction.class);
	    System.out.println("DataFrame schema");
	    txnsDF.printSchema();
	    System.out.println("Total # of rows: " + txnsDF.count());

	    txnsDF.createOrReplaceTempView("txns");
	    System.out.println("Big credits");
	    System.out.println("  SQL:");
	    Dataset<Row> bigCreditsSql = spark.sql("SELECT * FROM txns WHERE amount > 0 ORDER BY amount DESC LIMIT 10");
	    bigCreditsSql.show();
	    System.out.println("  DSL:");
	    Dataset<Row> bigCreditsDsl = txnsDF.filter("amount > 0").orderBy(col("amount").desc()).limit(10);
	    bigCreditsDsl.show();

	    System.out.println("Big debits");
	    System.out.println("  SQL:");
	    Dataset<Row> bigDebitsSql = spark.sql("SELECT * FROM txns WHERE amount < 0 ORDER BY amount LIMIT 10");
	    bigDebitsSql.show();
	    System.out.println("  DSL:");
	    Dataset<Row> bigDebitsDsl = txnsDF.filter("amount < 0").orderBy(col("amount")).limit(10);
	    bigDebitsDsl.show();

	    // defining and registering UDF function
	    spark.udf().register("len", (String s) -> s.length(), DataTypes.IntegerType);
	    Dataset<Row> longDesc = spark.sql("SELECT len(desc) AS desc_len, desc FROM txns WHERE len(desc) >= 100 ORDER BY len(desc) DESC");
	    System.out.println("Long descriptions");
	    for (Row row : longDesc.collectAsList()) {
		    System.out.println("Length: " + row.getAs("desc_len") + " Description: " + row.getAs("desc"));
	    }
	    System.out.println();

	    System.out.println("Big credits from JSON:");
	    Dataset<Row> jsonTxns = spark.read().json(srcDir + "/tx.jsons");
	    jsonTxns.createOrReplaceTempView("jtxs");

	    Dataset<Row> jsonBigCredits = spark.sql("SELECT * FROM jtxs WHERE amount > 0 ORDER BY amount DESC LIMIT 10");
	    jsonBigCredits.show();

	    spark.stop();
	    jsc.close();
	}


	public static void main(String[] args) throws Exception {
		new SparkSql().runJob();
	}
}
