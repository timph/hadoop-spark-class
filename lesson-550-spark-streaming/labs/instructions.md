# Spark Streaming

## Instructions

There are two labs for this lesson. In the first lab, we'll look at a stream of transactions and look for suspicious transactions. The second lab will illustrate the windowing feature of Spark Streams.

Stream processors typically run as daemons in the real world, so we decided for the Scala and Java guys to use more traditional methods than the notebooks to build and deploy these projects.

We'll also illustrate that you can do streaming through notebooks as well for the Python developers.

## Different way to run Notebooks

For the labs to work, we need to be able to stream data to the application.
We'll be using a simple approach of using netcat (a socket streaming tool).
This will allow us to push data into a socket that we're listening to inside our notebook.

If you use the notebooks for this lab, you need to run the notebooks a bit differently.

If you have notebooks up and running in docker already, please shutdown your existing instance. However, before you do, make sure that you save any work that you want to keep for later.

To shut down the docker image, you have to hit `Ctrl-C` followed by `y` to the shutdown prompt.

Next, we need to create a docker network. This is so that we can have netcat communicate with the running docker process.

Run the following command to create the network:

```sh
docker network create streamnet
```

Next, we'll run netcat using docker:

```sh
docker run -it --rm --name nc --network streamnet appropriate/nc -lk 9999
```

The above command has created a netcat process that runs on the newly created docker network. The netcat server is running on port 9999 and anything you type in this terminal will be streamed to anyone that have connected to this port.

Finally, (in a another terminal), we'll run Notebooks, but in this instance we'll make it use the docker network we setup.

```sh
docker run -it --rm --network streamnet -p 8888:8888 -v ${PWD}/lesson-5xx-resources:/home/jovyan/Resources -e SPARK_OPTS='--driver-memory 4g' jupyter/all-spark-notebook
```

That's it. You should now be setup to run the streaming lab.

## Accessing Lab Descriptions

* Lab 01: Suspicious Purchase Amount
  * Scala
      * To build a daemon use these instructions:  [lesson-550-spark-streaming/labs/lab-01-suspicious-purchase-amounts/scala/instructions.md](lab-01-suspicious-purchase-amounts/scala/instructions.md)

  * Python
      * We'll use notebooks
      * The notebook is here:
      [lesson-550-spark-streaming/labs/lab-01-suspicious-purchase-amounts/notebooks/python-550-lab-01-suspicious-purchase-amounts-window-operations.ipynb](lab-01-suspicious-purchase-amounts/notebooks/python-550-lab-01-suspicious-purchase-amounts-window-operations.ipynb)

* Lab 02: Window Operations
    * Scala
        * Build a daemon using these instructions: [lesson-550-spark-streaming/labs/lab-02-window-operations/scala/instructions.md](lab-02-window-operations/scala/instructions.md)
    * Python
        * Use this notebook: [lesson-550-spark-streaming/labs/lab-02-window-operations/notebooks/python-550-lab-02-suspicious-purchase-amounts-window-operations.ipynb](lab-02-window-operations/notebooks/python-550-lab-02-suspicious-purchase-amounts-window-operations.ipynb)

## Solutions

The solutions (which includes the Java code), can be found in the directory `lesson-550-spark-streaming/solutions`.
