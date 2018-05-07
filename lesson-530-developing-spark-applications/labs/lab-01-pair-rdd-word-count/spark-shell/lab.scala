// NOTE: set the value srcDir to a string containing the absolute path of
// the location of the source data directory, lesson-5xx-resources!

val file = srcDir + "/sherlock-holmes.txt";
val delimiters = Array(' ',',','.',':',';','?','!','-','@','#','(',')','\"','*');
val lines = sc.textFile(file);

// TODO: transform lines into tuples of (word, ???), then count tuples by key
val counts = ???;

val ifs = counts("if");
val ands = counts("and");
val buts = counts("but");
