
import org.apache.spark.sql._

object Test{

def mexicantakeout(filename: DataFrame):DataFrame ={
	val temp=filename.withColumn("categories", explode(col("categories")))
	val temp1=temp.filter(col("categories").equalTo("Mexican")).filter(col("attributes.RestaurantsTakeOut").equalTo("true"))
	temp1.groupBy("categories","attributes.RestaurantsTakeOut").avg("stars")
}
}