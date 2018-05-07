package com.scispike.spark;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;


import com.google.common.collect.ImmutableList;

public class SparkSqlAdvanced implements Serializable {

    private static final long serialVersionUID = 1l;

	private void runJob() {

	    SparkSession spark = SparkSession.builder().appName("SparkSqlAdvanced").getOrCreate();
	    spark.sparkContext().setLogLevel("ERROR");

	    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

	    List<Person> data = ImmutableList.of(new Person("Jane", "F", 25), new Person("Frank", "M", 30), new Person("Eve", "F", 45), new Person("Andrew", "M", 18));

	    JavaRDD<Person> people = jsc.parallelize(data);

	    System.out.println("List of all people");
	    List<Person> output = people.collect();
	    for (Person person : output) {
	      System.out.println(person.getName()  + " " + person.getSex() + " " + person.getAge());
	    }

	    Dataset<Row> peopleDF = spark.createDataFrame(data, Person.class);
	    System.out.println("DataFrame schema");
	    peopleDF.printSchema();
	    System.out.println("DataFrame show");
	    peopleDF.show();

	    peopleDF.createOrReplaceTempView("people");

	    System.out.println("List of all pairs of males and females where male is same age or older than female but no more than 5 years");
	    Dataset<Row> pairs = spark.sql("SELECT * FROM people p1 JOIN people p2 ON p1.age-p2.age BETWEEN 0 AND 5 WHERE p1.sex='M' AND p2.sex='F'");
	    pairs.show();

	    // defining and registering UDF function
	    spark.udf().register("strLen", (String s) -> s.length(), DataTypes.IntegerType);

	    Dataset<Row> length = spark.sql("SELECT name, strLen(name) AS length FROM people");
	    System.out.println("List of all names with their lengths");
	    length.show();

	    spark.udf().register("teenager", (Integer s) -> (s >= 13 && s <= 19), DataTypes.BooleanType);

	    Dataset<Row> teens = spark.sql("SELECT name, teenager(age) AS teen FROM people");
	    System.out.println("List of all names with teenage flag");
		teens.show();

		// applying UDF in DataFrame
		Dataset<Row> teens2 = peopleDF.select(col("name"), callUDF("teenager", col("age")).alias("teen"));
		teens2.show();

	    System.out.println("List of teenagers after applying filter on DataFrame");
		Dataset<Row> teens3 = peopleDF.filter(callUDF("teenager", col("age")));
		teens3.show();

	    spark.stop();
	    jsc.close();
	}


	public static void main(String[] args) throws Exception {
		new SparkSqlAdvanced().runJob();
	}
}
