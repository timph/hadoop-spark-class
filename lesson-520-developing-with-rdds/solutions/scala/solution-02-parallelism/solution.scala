// NOTE: set the value srcDir to a string containing the absolute path of
// the location of the source data directory, lesson-5xx-resources!

val minCores = 2 // if your system has 2+ cores
val nCores = 8 // depends on your system
val partitions = 2 to nCores by 2
val files = Array("byu-corpus.txt"/*, "sherlock-holmes.txt"*/);
val delimiters = Array(' ',',','.',':',';','?','!','-','@','#','(',')');
files foreach( file => {
  val original = sc.textFile(srcDir + "/" + file);
  println("File: " + file);
  partitions foreach(n => {
    println("Partitions: " + n);
    val partitioned = original.repartition(n);
    val start = System.currentTimeMillis;
    val result = partitioned.flatMap(_.split(delimiters)).filter(_.trim.length > 0).map(x => (x, 1)).reduceByKey(_ + _).sortBy(_._2, false).first;
    val time = System.currentTimeMillis - start;
    println("Took: " + time + " ms");
  });
});
