// NOTE: set the value srcDir to a string containing the absolute path of
// the location of the source data directory, lesson-5xx-resources!

val file = srcDir + "/sherlock-holmes.txt";
val delimiters = Array(' ',',','.',':',';','?','!','-','@','#','(',')','\"','*');
val lines = sc.textFile(file);

val words = lines.flatMap(_.split(delimiters));
val pairs = words.map(word => (word.toLowerCase, None));
val counts = pairs.countByKey();

val ifs = counts("if");
val ands = counts("and");
val buts = counts("but");
