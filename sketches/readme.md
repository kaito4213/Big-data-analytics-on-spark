
### Count Min SKetch Implementation on Spark

#### You need to pre-install
* Hadoop 2.7.0
* Spark 2.0.0
* Scala 2.11.8
* Python 3.5

#### Developer Environment
* Linux + pyspark + jupyter notebook + python3

First install Jupyter Notebook: $ pip3 install jupyter

Then configure your $PATH variables by adding the following lines in your ~/.bashrc file:

$ export SPARK_HOME=/usr/local/spark(where you install spark)

$ export PATH=$SPARK_HOME/bin:$PATH

Next Update PySpark driver environment variables: add these lines to your ~/.bashrc file:

$ export PYSPARK_DRIVER_PYTHON=jupyter

$ export PYSPARK_DRIVER_PYTHON_OPTS='notebook'

Now lauch pyspark $ pyspark

This command should start a Jupyter Notebook in your web browser. 

reference blog: https://blog.sicara.com/get-started-pyspark-jupyter-guide-tutorial-ae2fe84f594f

#### result
Open the file CountMinSketch in jupyter and run the code cells.

eg. Give the input delta = 0.07, epsilon = 0.03, incrementFile = 'file:///usr/local/spark/README.md'

the output is:

The number of hash functions are: 3
The number of buckets for each hash function are: 91


Here is the count table after increment:
[[  3.   1.   4.  10.   5.   9.   1.   4.   4.   6.   4.   6.   1.   5.
    7.   6.  15.   2.   6.   2.   2.   3.   2.   0.  13.   2.   2.   3.
   15.   6.   2.  13.   4.   1.  72.   2.  15.   1.   2.   3.   3.   5.
    3.   9.   1.  10.   6.   4.   6.   2.   0.  14.   4.  10.   6.   3.
    4.   4.   2.   4.   4.   4.   6.   2.   4.  15.  22.   5.   3.   2.
    1.   6.   5.   9.   4.   5.   2.   1.  25.   2.   1.   4.   1.   2.
    6.   9.   5.   4.   8.   5.   4.]
 [  2.   4.   2.   5.  11.   5.  17.   3.   2.   7.   4.  10.   4.   1.
    5.   4.   9.   1.   3.   3.   1.   1.   4.   6.   8.   3.   6.   2.
   11.   4.   4.   5.   9.   4.   2.   3.   7.   3.   3.   2.   2.   4.
    5.   2.   1.   1.   7.   9.  10.   0.   3.   4.   0.   4.   2.  25.
    2.   4.   3.   6.   0.   6.   0.   3.   1.   4.  16.   5.   7.   8.
    1.   4.   6.   2.   1.   6.   2.  15.  72.   5.  16.  17.  24.   2.
    4.   1.   3.   5.  12.   1.   7.]
 [  8.   2.   7.   1.   6.   4.   2.   3.  15.   3.   5.  10.   0.   3.
    2.   2.   9.   0.  11.   5.   3.   1.   5.   4.   2.   1.   3.   8.
    5.   3.   0.   0.   5.   4.  16.   1.   3.   7.   3.   2.   5.   9.
    2.   2.  16.   1.   3.   7.   5.   4.  11.   5.   3.   2.   2.   3.
   13.  13.  31.   4.   0.   3.  12.   2.   3.   7.   8.   5.   4.   2.
    6.  17.   5.  11.   6.   3.   2.   0.   3.   1.   1.   6.   4.   3.
   16.   4.   3.   3.  74.   7.   4.]]


Now we merge the same count table by loading the same file.
 The count table should be doubled
[[   6.    2.    8.   20.   10.   18.    2.    8.    8.   12.    8.   12.
     2.   10.   14.   12.   30.    4.   12.    4.    4.    6.    4.    0.
    26.    4.    4.    6.   30.   12.    4.   26.    8.    2.  144.    4.
    30.    2.    4.    6.    6.   10.    6.   18.    2.   20.   12.    8.
    12.    4.    0.   28.    8.   20.   12.    6.    8.    8.    4.    8.
     8.    8.   12.    4.    8.   30.   44.   10.    6.    4.    2.   12.
    10.   18.    8.   10.    4.    2.   50.    4.    2.    8.    2.    4.
    12.   18.   10.    8.   16.   10.    8.]
 [   4.    8.    4.   10.   22.   10.   34.    6.    4.   14.    8.   20.
     8.    2.   10.    8.   18.    2.    6.    6.    2.    2.    8.   12.
    16.    6.   12.    4.   22.    8.    8.   10.   18.    8.    4.    6.
    14.    6.    6.    4.    4.    8.   10.    4.    2.    2.   14.   18.
    20.    0.    6.    8.    0.    8.    4.   50.    4.    8.    6.   12.
     0.   12.    0.    6.    2.    8.   32.   10.   14.   16.    2.    8.
    12.    4.    2.   12.    4.   30.  144.   10.   32.   34.   48.    4.
     8.    2.    6.   10.   24.    2.   14.]
 [  16.    4.   14.    2.   12.    8.    4.    6.   30.    6.   10.   20.
     0.    6.    4.    4.   18.    0.   22.   10.    6.    2.   10.    8.
     4.    2.    6.   16.   10.    6.    0.    0.   10.    8.   32.    2.
     6.   14.    6.    4.   10.   18.    4.    4.   32.    2.    6.   14.
    10.    8.   22.   10.    6.    4.    4.    6.   26.   26.   62.    8.
     0.    6.   24.    4.    6.   14.   16.   10.    8.    4.   12.   34.
    10.   22.   12.    6.    4.    0.    6.    2.    2.   12.    8.    6.
    32.    8.    6.    6.  148.   14.    8.]]


Now estimate the times of appearance for each word in given test file:

[('implementation', 0.0), ('this', 6.0), ('is', 12.0), ('python.', 8.0), ('min', 2.0), ('for', 26.0), ('test', 2.0), ('spark', 8.0), ('file', 12.0), ('sketch', 0.0), ('count', 8.0), ('on', 14.0), ('using', 14.0), ('hello', 2.0)]

You can open the notebook and change the parameter accordingly.

