# Spark_Optimization_Mini_Proj
### Description:
Spark can give you a tremendous advantage when it comes to quickly processing massive datasets. However, the tool is only as powerful as the one who wields it. Spark performance can become sluggish if poor decisions are made in the layout of the code and functions that are chosen.

This exercise gives hands-on experience optimizing PySpark code.I will look at the
physical plan for the query execution and then modify the query to improve performance.

### How I improved the performance of this queary:
I started by timing the initial queary which came out to about 4.3 seconds to process. I then pulled up the queary plan with the ``` resultDF.explain()``` function. I saw that the partitions was where the quary was bottlenecking so I decided to repartition by month in order to reduce shuffling. I timed this adjusted quary and it came out to about 1 second.  
