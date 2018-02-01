import org.apache.spark.sql._
import scala.math._

object Test{

def foodNearToronto(filename: DataFrame)(filename2: DataFrame):DataFrame={
	val temp=filename.withColumn("categories", explode(col("categories")))
	val temp0 = temp.filter(col("latitude").isNotNull).filter(col("longitude").isNotNull)
	val temp1 = temp0.select("business_id","latitude","longitude","city","categories","stars","review_count","name")
	val temp2 = temp1.map(r=>(r.getString(0),(acos((sin(43.6532*Pi/180)*sin(r.getDouble(1)*Pi/180))+(cos(r.getDouble(1)*Pi/180)*cos(43.6532*Pi/180)*cos((79.3832*Pi*(-1)/180)-(r.getDouble(2)*Pi/180))))*6371),r.getString(3),r.getString(4),r.getDouble(5),r.getLong(6), r.getString(7)))
	val temp3 = temp2.toDF("business_id","distance","city","categories","stars","review_count","name")
	val temp4 = temp3.filter($"distance"<=15)
	val datafilter = temp4.filter(col("categories").equalTo("Food"))
	val temp41 = datafilter.sort(col("stars").desc).take(10)
	val temp411 = temp41.map(a=>(a.getString(0),a.getDouble(1),a.getString(2),a.getString(3),a.getDouble(4),a.getLong(5),a.getString(6)))
	val df1 = sc.parallelize(temp411).toDF("business_id","distance","city","categories","stars","review_count","name")
	val temp42 = datafilter.sort(col("stars").asc).take(10)
	val temp412 = temp42.map(a=>(a.getString(0),a.getDouble(1),a.getString(2),a.getString(3),a.getDouble(4),a.getLong(5),a.getString(6)))
	val df2 = sc.parallelize(temp412).toDF("business_id","distance","city","categories","stars","review_count","name")
	val temp44 = df1.union(df2)
	val temp413 = temp44.drop("stars")
	
	val datajoin1 = temp413.join(filename2, Seq("business_id"))
	val datajoinUP = datajoin1.filter(col("date").contains("-01-")||col("date").contains("-02-")||col("date").contains("-03-")||col("date").contains("-04-")||col("date").contains("-05"))
	datajoinUP.groupBy("name").avg("stars")
	
}
}