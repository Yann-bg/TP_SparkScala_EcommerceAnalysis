import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Etape1TransformationData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Ecommerce Data Preprocessing")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val filePath = "ecommerce_data.csv"

    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    println("Aperçu des données brutes :")
    rawDF.show(10)

    val cleanedDF = rawDF
      .filter($"user_id".isNotNull &&
        $"session_duration".isNotNull && $"session_duration" > 0 &&
        $"pages_viewed".isNotNull && $"pages_viewed" > 0 &&
        $"purchase_amount".isNotNull && $"purchase_amount" >= 0 &&
        $"review_score".isNotNull && $"review_score".between(1, 5) &&
        $"timestamp".isNotNull)

    println("Données nettoyées :")
    cleanedDF.show(10)

    val transformedDF = cleanedDF
      .withColumn("hour", hour(to_timestamp($"timestamp")))
      .withColumn("day_of_week", date_format(to_date($"timestamp"), "E"))
      .withColumn("device_type", lower($"device_type"))
      .withColumn("long_session", $"session_duration" > 60)

    println("Données transformées :")
    transformedDF.show(10)
    transformedDF.printSchema()

    transformedDF
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", ",")
      .mode("overwrite")
      .csv("Ecommerce/ecommerce_transformed")

    spark.stop()
  }
}