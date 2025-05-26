import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Etape2Analyse {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Ecommerce Data Analysis")
      .master("local[*]")
      .getOrCreate()


    val filePath = "Ecommerce/ecommerce_transformed.csv"

    val transformedDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    println("Aperçu des données transformées :")
    transformedDF.show(10)
    transformedDF.printSchema()

    val sessionsParAppareil = transformedDF
      .groupBy("device_type")
      .count()
      .orderBy(desc("count"))

    println("Sessions par type d'appareil :")
    sessionsParAppareil.show()

    val achatsParCategorie = transformedDF
      .groupBy("product_category")
      .agg(round(sum("purchase_amount"), 2).alias("total_achats"))
      .orderBy(desc("total_achats"))

    println("Achats par catégorie de produit :")
    achatsParCategorie.show()

    val noteParPays = transformedDF
      .groupBy("country")
      .agg(round(avg("review_score"), 2).alias("note_moyenne"))
      .orderBy(desc("note_moyenne"))

    println("Note moyenne par pays :")
    noteParPays.show()

    val dureeMoyenneSession = transformedDF
      .groupBy("long_session")
      .agg(round(avg("session_duration"), 2).alias("duree_moyenne"))

    println("Durée moyenne selon type de session :")
    dureeMoyenneSession.show()

    val repartitionAvis = transformedDF
      .groupBy("review_score")
      .count()
      .orderBy("review_score")

    println("Répartition des avis :")
    repartitionAvis.show()

    val activiteParHeure = transformedDF
      .groupBy("hour")
      .count()
      .orderBy("hour")

    println("Activité par heure :")
    activiteParHeure.show()

    val sessionsParJour = transformedDF
      .groupBy("day_of_week")
      .count()
      .orderBy(desc("count"))

    println("Sessions par jour de la semaine :")
    sessionsParJour.show()

    spark.stop()
  }
}
