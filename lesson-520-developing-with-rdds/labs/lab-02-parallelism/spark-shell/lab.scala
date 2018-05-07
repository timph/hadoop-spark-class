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

    // TODO: repartition the RDD to use n cores
    val partitioned = ???;

    val start = System.currentTimeMillis;

    // TODO: find the word used the most in the file using RDD called "partitioned"
    val result = partitioned.???;

    val time = System.currentTimeMillis - start;
    println("Took: " + time + " ms");
  });
});
