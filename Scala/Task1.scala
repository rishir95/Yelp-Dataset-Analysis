import org.apache.spark.sql._

object Test{

def avgrevstar(filename: DataFrame):DataFrame ={
	
	filename.withColumn("categories", explode(col("categories"))).groupBy("city","categories").avg("stars","review_count")
}
}