# spark-course

## Course material

The course material is arranged in directories where we have one directory for each lesson. In addition, we also have directories for the data sources and Jupyter Notebooks.

The data is arranged as follows:

* Individual lessons:
  * `lesson-5{Lesson number}-{Lesson description}/`
    * `slides/` contains the PowerPoint presentation
    * `labs/` contains all the lab description and lab starting points
    * `solutions/` suggestion for solutions
* Data files
  * `lesson-5xx-resources/`
* Notebooks
  * `notebooks/`

## Outline of the course

This course is spread over a set of directories representing different lessons.

Each lesson is named `lesson-5xx-description` where `xx` is some number and the `description` describes the topic.

The topics are:

* Lesson 500: Spark Overview
    * A simple introduction to Spark
        * [Slides in PowerPoint format](lesson-500-spark-overview/slides/spark-overview.pptx)
* Lesson 510: Hello Spark
    * A simple lab illustrating how to run Spark
    * [Lab instructions](lesson-510-hello-spark/labs/instructions.md)
* Lesson 520: Developing with Rdds
    * A discussion of the fundamental Resilient Distributed Dataset of Spark
    * [Slides in PowerPoint format](lesson-520-developing-with-rdds/slides/developing-with-rdds.pptx)
    * [Lab Instructions](lesson-520-developing-with-rdds/labs/instructions.md)
* Lesson 530: Developing with Scala or Python or Java
    * We'll be looking deeper at how to use the language of your choice to develop Spark Applications
    * [Slides in PowerPoint format](lesson-520-spark-overview/slides/spark-overview.pptx)
    * [Lab instructions](lesson-530-developing-spark-applications/labs/instructions.md)
* Lesson 540: Test Driven Development with Spark
    * This is an optional chapter where we'll discuss how to do test driven development with Spark
    * [Slides in PowerPoint](lesson-540-test-driven-development-with-spark/slides/test-driven-development-with-spark.pptx)
    * [Lab Instructions](lesson-540-test-driven-development-with-spark/labs/instructions.md)
* Lesson 550: Spark Streaming
    * In this chapter we will be exploring how to do stream processing with Spark
    * [Slides in PowerPoint](lesson-550-spark-streaming/slides/spark-streaming.pptx)
    * [Lab Instructions](lesson-550-spark-streaming/labs/instructions.md)
* Lesson 560: Spark SQL
    * In this chapter we'll introduce Spark SQL
    * [Slides in PowerPoint](lesson-560-spark-sql/slides/spark-sql.pptx)
    * [Lab instructions](lesson-560-spark-sql/labs/instructions.md)
* Lesson 570: MLIB and GraphX
    * In this chapter we'll explore the machine learning and graph capabilities of Spark
    * [Slides in PowerPoint](lesson-570-mlib-graphx/slides/spark-mlib-graphx.pptx)
    * [Lab Instructions](lesson-570-mllib-graphx/labs/instructions.md)

## Installation Instruction

There are many ways to execute Spark. You can of course a JVM based program, use the Spark REPL and there is also various developer notebooks supported.

In addition to the environment, developers may use Scala, Java or Python.

In this course, we recommend that you use Jupyter Notebooks, but you can also decide to run Spark natively if you so please.

Either way, a bit of installation and configuration may be required to get started.

## Selecting a Development Environment

Let us try to guide you so that you can select the right approach to this course.

### I'm a Data Scientist

If you are a data scientist and your primary responsibilities in your projects are to reason over data, build queries and provide insights, we would strongly recommend that you use the Jupyter Notebooks.

You will have a choice to run the notebooks in Scala or Python. Python seems to be the preferred language among data scientists, but Scala is also a very good option. Spark was written in Scala, so the Scala API is perhaps a bit richer (in particular, some of the more advanced Spark libraries don't have Python binding yet).

If you don't care which language, pick Python. If you are uncertain, I suggest you discuss with the instructor your particular situation and maybe he/she can help you select.  

### I'm a Developer

If you are a developer, you probably have a strong preference between Python, Java or Scala. Let us make some recommendations based on your primary language.

* ***Java Developers***. Ideally, we would want you to use a Jupyter Notebook for Java, but a Java-based notebook is only in incubation and only for Java 9, so we recommend that you use the Jupyter Notebooks with Scala. Scala will feel more natural to you than Python (after all, it is a language targeting the JVM). The advantage of using a Notebook will be clear as you do the exercises. We have provided solutions to all exercises in Java, so you may want to browse, compile and run the Java code after you made something run in the Notebook.
* *** Scala Developers ***. Use the Scala Notebooks. You could theoretically use your favorite IDE and SBT, but we recommend you use the Jupyter Notebooks and Scala.
* *** Python Developers ***. The choice here is obvious. Use the Jupyter Notebooks for Python.

## Installation of Docker

We'll run the notebooks using Docker.
If you don't have Docker installed already, please go to https://docs.docker.com/install/ to get the installation instructions.

Please follow the documentation from the link above to install docker.
The Community Edition is sufficient to run the labs.

When you're finished installing Docker, please run the following command:

```bash
docker run hello-world
```

If you are successful, you should (potentially after some download time) see something like this:

```shell
$ docker run hello-world

Hello from Docker!
This message shows that your installation appears to be working correctly.

To generate this message, Docker took the following steps:
 1. The Docker client contacted the Docker daemon.
 2. The Docker daemon pulled the "hello-world" image from the Docker Hub.
    (amd64)
 3. The Docker daemon created a new container from that image which runs the
    executable that produces the output you are currently reading.
 4. The Docker daemon streamed that output to the Docker client, which sent it
    to your terminal.

To try something more ambitious, you can run an Ubuntu container with:
 $ docker run -it ubuntu bash

Share images, automate workflows, and more with a free Docker ID:
 https://cloud.docker.com/

For more examples and ideas, visit:
 https://docs.docker.com/engine/userguide/

$
```

## Running the notebooks

We have separated out the instructions for how to run the Jupyter notebooks. We did so as you probably will have to come back to that file multiple times

The notebook instructions can be found in [the file running-notebooks.md](running-notebooks.md)

Congratulations! You are all set for the labs.
