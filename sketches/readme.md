# Sketching data structures: Bloom Filter and Count-Min
### Environment
- Hadoop 2.7.0
- Spark 2.0.0
- Scala 2.11.8
- Python 3.5
- Linux + pyspark + jupyter notebook

### Set up
- First install Jupyter Notebook: ```$ pip3 install jupyter```
- Then configure your $PATH variables by adding the following lines in your ```~/.bashrc file```:
```$ export SPARK_HOME=/usr/local/spark(where you install spark)```

```$ export PATH=$SPARK_HOME/bin:$PATH```

Next Update PySpark driver environment variables: add these lines to your ```~/.bashrc file```:
```$ export PYSPARK_DRIVER_PYTHON=jupyter```

```$ export PYSPARK_DRIVER_PYTHON_OPTS='notebook'```

Now lauch pyspark ```$ pyspark```

This command should start a Jupyter Notebook in your web browser. 
reference blog: https://blog.sicara.com/get-started-pyspark-jupyter-guide-tutorial-ae2fe84f594f

### result
Open the file CountMinSketch in jupyter and run the code cells.
eg. Give the input delta = 0.07, epsilon = 0.03, incrementFile = 'file:///usr/local/spark/README.md'
The number of hash functions are: 3
The number of buckets for each hash function are: 91
Estimate the times of appearance for each word in given test file:
[('implementation', 0.0), ('this', 6.0), ('is', 12.0), ('python.', 8.0), ('min', 2.0), ('for', 26.0), ('test', 2.0), ('spark', 8.0), ('file', 12.0), ('sketch', 0.0), ('count', 8.0), ('on', 14.0), ('using', 14.0), ('hello', 2.0)]
You can open the notebook and change the parameter accordingly.

