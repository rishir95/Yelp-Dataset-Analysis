import org.apache.spark.sql._

object Test{

def pivotavg(filename: DataFrame):DataFrame ={
	val temp=filename.withColumn("categories", explode(col("categories")))
	temp.groupBy("city","state").pivot("categories").avg("stars")
}
}