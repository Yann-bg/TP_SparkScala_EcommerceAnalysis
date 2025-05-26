import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.knowm.xchart._
import org.knowm.xchart.style.markers.SeriesMarkers
import org.knowm.xchart.XYSeries

import java.awt.{Color, BasicStroke}
import scala.collection.JavaConverters._

object Etape3Visualizations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Ecommerce Advanced Visualizations")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("encoding", "UTF-8")
      .csv("Ecommerce/ecommerce_transformed")

    val metrics = prepareMetrics(df)

    val charts = Seq(
      createDeviceTypeChart(metrics.deviceDistribution),
      createCategoryRevenueChart(metrics.categoryRevenue),
      createReviewScoreChart(metrics.reviewDistribution),
      createHourlyActivityChart(metrics.hourlyActivity),
      createCountryReviewChart(metrics.countryReviews),
      createSessionDurationChart(metrics.sessionMetrics),

    )

    new SwingWrapper(charts.asJava).displayChartMatrix()
    spark.stop()
  }

  case class Metrics(
                      deviceDistribution: Array[(String, Long)],
                      categoryRevenue: Array[(String, Double)],
                      reviewDistribution: Array[(Double, Long)],
                      hourlyActivity: Array[(Int, Long)],
                      countryReviews: Array[(String, Double)],
                      sessionMetrics: (Array[String], Array[Double]),
                    )

  def prepareMetrics(df: org.apache.spark.sql.DataFrame): Metrics = {
    val deviceData = df.groupBy("device_type").count().orderBy(desc("count"))
      .collect().map(r => (r.getString(0), r.getLong(1)))

    val categoryData = df.groupBy("product_category").agg(sum("purchase_amount").as("total"))
      .orderBy(desc("total")).collect().map(r => (r.getString(0), r.getDouble(1)))

    val reviewData = df.groupBy("review_score").count().orderBy("review_score")
      .collect().map(r => (r.getDouble(0), r.getLong(1)))

    val hourlyData = df.groupBy("hour").count().orderBy("hour")
      .collect().map(r => (r.getInt(0), r.getLong(1)))

    val countryData = df.groupBy("country")
      .agg(sum("purchase_amount").as("total_sales"))
      .orderBy(desc("total_sales"))
      .limit(10)
      .collect()
      .map(row => (row.getString(0), row.getDouble(1)))

    val sessionData = df.groupBy("long_session").agg(avg("session_duration").as("avg_duration"))
      .collect()
    val sessionTypes = sessionData.map(r => r.getBoolean(0).toString)
    val avgDurations = sessionData.map(r => r.getDouble(1))

    val weeklyActivity = df.groupBy("day_of_week").count().orderBy("day_of_week")
      .collect().map(r => (r.getString(0), r.getLong(1)))

    val deviceSession = df.groupBy("device_type").agg(avg("session_duration").as("avg"))
      .orderBy(desc("avg")).collect().map(r => (r.getString(0), r.getDouble(1)))

    val categoryPurchase = df.groupBy("product_category").count()
      .orderBy(desc("count")).collect().map(r => (r.getString(0), r.getLong(1)))

    val avgScoreByCountry = df.groupBy("country").agg(avg("review_score").as("avg"))
      .orderBy(desc("avg")).collect().map(r => (r.getString(0), r.getDouble(1)))

    Metrics(
      deviceData,
      categoryData,
      reviewData,
      hourlyData,
      countryData,
      (sessionTypes, avgDurations)
    )
  }

  def createDeviceTypeChart(data: Array[(String, Long)]): PieChart = {
    val chart = new PieChartBuilder().width(600).height(400).title("Répartition par type d'appareil").build()
    data.foreach { case (device, count) => chart.addSeries(s"$device ($count)", count) }
    chart.getStyler.setLegendVisible(true)
    chart.getStyler.setPlotContentSize(0.7)
    chart
  }

  def createCategoryRevenueChart(data: Array[(String, Double)]): PieChart = {
    val chart = new PieChartBuilder().width(800).height(500)
      .title("Revenus par catégorie de produit").build()

    data.foreach { case (category, revenue) =>
      chart.addSeries(s"$category (${f"$revenue%.2f"}€)", revenue)
    }

    chart.getStyler.setLegendVisible(true)
    chart.getStyler.setPlotContentSize(0.7)
    chart
  }


  def createReviewScoreChart(data: Array[(Double, Long)]): CategoryChart = {
    val chart = new CategoryChartBuilder().width(700).height(500)
      .title("Répartition des notes")
      .xAxisTitle("Note")
      .yAxisTitle("Nombre d'avis")
      .build()
    chart.addSeries("Reviews", data.map(_._1.toString).toList.asJava, data.map(_._2.toDouble).map(Double.box).toList.asJava)
    chart.getStyler.setAvailableSpaceFill(0.3)
    chart
  }

  def createHourlyActivityChart(data: Array[(Int, Long)]): XYChart = {
    val chart = new XYChartBuilder().width(900).height(500)
      .title("Activité horaire")
      .xAxisTitle("Heure de la journée")
      .yAxisTitle("Nombre de sessions")
      .build()
    val series = chart.addSeries("Sessions", data.map(_._1.toDouble), data.map(_._2.toDouble))
    series.setMarker(SeriesMarkers.CIRCLE)
    series.setLineColor(new Color(70, 130, 180))
    series.setLineStyle(new BasicStroke(2.0f))
    chart.getStyler.setMarkerSize(8)
    chart.getStyler.setCursorEnabled(true)
    chart.getStyler.setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Line)
    chart
  }

  def createCountryReviewChart(data: Array[(String, Double)]): CategoryChart = {
    val chart = new CategoryChartBuilder().width(800).height(500)
      .title("Top 10 des pays par ventes")
      .xAxisTitle("Pays")
      .yAxisTitle("Ventes totales (€)")
      .build()
    chart.addSeries("Scores", data.map(_._1).toList.asJava, data.map(_._2).map(Double.box).toList.asJava)
    chart.getStyler.setXAxisLabelRotation(45)
    chart.getStyler.setAvailableSpaceFill(0.4)
    chart
  }

  def createSessionDurationChart(data: (Array[String], Array[Double])): CategoryChart = {
    val chart = new CategoryChartBuilder().width(600).height(400)
      .title("Durée moyenne des sessions")
      .xAxisTitle("Type de session")
      .yAxisTitle("Durée (minutes)")
      .build()
    chart.addSeries("Duration", data._1.toList.asJava, data._2.map(Double.box).toList.asJava)
    chart.getStyler.setAvailableSpaceFill(0.3)
    chart
  }

}
