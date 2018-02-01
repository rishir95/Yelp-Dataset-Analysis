---------------------------
Table of Contents
---------------------------

A. Loading .scala file
B. Executing .scala file

----------------------------
A. Loading .scala file
----------------------------
In order to load the .scala file the ":load" command must be used along with the path of the file.

e.g 	if the spark is installed in C directory and the .scala file is located at c\yelp_dataset\prog\Taask.scala.	
		The command to load the file will be :
		
		scala> :load /yelp_dataset/prog/Task.scala
		
----------------------------
B. Executing .scala file
----------------------------
Once the files have been loaded the functions can be simply run by passing the parameter.
1.	The dataset must be created for "business.json" and "review.json".
2.	After the dataset has been created the dataframe must be passed to the functions.

scala> val data=spark.read.json("C:/yelp_dataset/dataset/business.json")
scala> val data1=spark.read.json("Z:/yelp_dataset/dataset/review.json")

For Task 1
'''
scala> :load /yelp_dataset/prog/Task.scala
scala> val abc=Test.avgrevstar(data)
'''

For Task 2:
scala> :load /yelp_dataset/prog/Task2.scala
scala> val abc=Test.pivotavg(data)

For Task 3:
scala> :load /yelp_dataset/prog/Task3.scala
scala> val abc=Test.mexicantakeout(data)  

For Task 4:
scala> :load /yelp_dataset/prog/Task4.scala
scala> val abc=Test.nearToronto(data)

For Task 5:
scala> :load /yelp_dataset/prog/Task5.scala
scala> val abc=Test.foodNearToronto(data)(data1)
