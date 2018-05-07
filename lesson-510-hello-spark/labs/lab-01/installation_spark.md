# Spark Installation


## Mac & Linux

If you are running on Mac or Linux, unzip the archive. Set the environment variable
```
export SPARK_HOME=<spark_location>
```

**Note:** On some PayPal machines, there is an exception when you start Spark. The remedy is to add an environment variable:

```
export SPARK_LOCAL_IP=127.0.0.1
```

That's it!


## Windows

Installing Spark on Windows is more involving. There is a notorious issue that is not automatically solved, but there is a workaround. If you are curious, check this [Jira entry](https://issues.apache.org/jira/browse/SPARK-10528).

### Spark
Unzip the Spark distribution. Set the environment variable `SPARK_HOME` to the directory where you unzipped Spark.

### Java
Download and install Java. You need Java 1.7 or 1.8.
Set the environment variable `JAVA_HOME` to the Java directory.

### Winutils
This file contains utilities that are needed to run Spark and Hadoop. Official documentation is advising to build the executable using Visual Studio, which is cumbersome. Fortunately, we can download a built version from [here](
https://github.com/steveloughran/winutils/raw/master/hadoop-2.6.0/bin/winutils.exe).

Place the downloaded file in directory `C:\Hadoop\bin`.

Create the environment variable  `HADOOP_HOME` and set it to: `C:\Hadoop`

Edit your `PATH` environment variable.

Add this to the end:
`;%SPARK_HOME%\bin;%HADOOP_HOME%\bin;%JAVA_HOME%\bin;`

### Set up temporary Hive folder

Create folder `C:\tmp\hive`.

Start `command` window in Admin mode. Run:

`winutils.exe chmod -R 777 \tmp\hive`


### General note: Windows file names in Scala programs
When giving file names in Scala strings, remember to use `\\` where you would normally use `\`. This is coming from Java escape sequences in strings. Example: `C:\\file.txt`.
