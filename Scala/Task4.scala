import org.apache.spark.sql._
import scala.math._

object Test{

def nearToronto(filename: DataFrame):DataFrame ={
	val temp=filename.withColumn("categories", explode(col("categories")))
	val temp0 = temp.filter(col("latitude").isNotNull).filter(col("longitude").isNotNull)
	val temp1 = temp0.select("latitude","longitude","city","categories","stars","review_count")
	val temp2 = temp1.map(r=>((acos((sin(43.6532*Pi/180)*sin(r.getDouble(0)*Pi/180))+(cos(r.getDouble(0)*Pi/180)*cos(43.6532*Pi/180)*cos((79.3832*Pi*(-1)/180)-(r.getDouble(1)*Pi/180))))*6371),r.getString(2),r.getString(3),r.getDouble(4),r.getLong(5)))
	val temp3 = temp2.toDF("distance","city","categories","stars","review_count")
	val temp4 = temp3.filter(col("distance")<=15)
	
	temp4.groupBy("categories").avg("stars","review_count")
	
}
}