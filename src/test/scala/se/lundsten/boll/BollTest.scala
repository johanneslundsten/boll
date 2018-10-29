package se.lundsten.boll

import java.sql.Timestamp

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.junit.Test
import org.apache.spark.sql.functions._

class BollTest {

  @Test
  def test(): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    val team = "Barcelona"
    val opponent = "Real Madrid"
    val date = "2018-10-18"

    /*
    +-----------+-----------+------------+--------------+------+-------------------+------------+
    |       team|   opponent|scored_goals|conceded_goals|points|               date|home_or_away|
    +-----------+-----------+------------+--------------+------+-------------------+------------+
    |    Leganes|     Alaves|           1|             0|     3|2017-08-18 00:00:00|        home|
     */
    val allGames = createAllGamesFromFile()

    val allTables = createAllTables(allGames)

    val windowSpec = Window.partitionBy("team").orderBy($"date".desc)

    val lastTen = allGames
      .withColumn("rn", row_number().over(windowSpec))

    lastTen
      .where($"rn" <= 10)
      .groupBy("team")
      .agg(
        sum($"scored_goals"),
        sum($"conceded_goals"),
        avg($"points")
      ).show(100)

    lastTen
      .where($"rn" <= 5)
      .groupBy("team")
      .agg(
        sum($"scored_goals"),
        sum($"conceded_goals"),
        avg($"points")
      ).show(100)

  }

  def createAllGamesFromFile() : DataFrame = {
    val sparkSession = SparkSession.builder()
      .getOrCreate()

    import sparkSession.implicits._

    val df = sparkSession.read
      .option("header", value = true)
      .csv("C:/git/boll/data/*/*")
      .withColumn("season", substring(split(input_file_name(), "/").getItem(8), 0, 9))
      .where($"div" === "SP1" && $"season" === "2017_2018")
      .withColumn("Date", to_timestamp($"Date", "dd/MM/yy"))
      .select(
        $"Div".as("league"),
        $"date",
        $"HomeTeam".as("home_team"),
        $"AwayTeam".as("away_team"),
        $"FTHG".as("home_goals"),
        $"FTAG".as("away_goals"),
        $"FTR".as("result"))
      .persist(StorageLevels.MEMORY_AND_DISK_2)
    //      .printSchema()


    val homeGames = df.select(
      $"home_team".as("team"),
      $"away_team".as("opponent"),
      $"home_goals".as("scored_goals"),
      $"away_goals".as("conceded_goals"),
      when($"result" === "H", 3).otherwise(when($"result" === "D", 1).otherwise(0)).as("points"),
      $"date",
      lit("home").as("home_or_away")
    )

    val awayGames = df.select(
      $"away_team".as("team"),
      $"home_team".as("opponent"),
      $"away_goals".as("scored_goals"),
      $"home_goals".as("conceded_goals"),
      when($"result" === "A", 3).otherwise(when($"result" === "D", 1).otherwise(0)).as("points"),
      $"date",
      lit("away").as("home_or_away"))

    val allGames = homeGames.union(awayGames)
      .persist(StorageLevels.MEMORY_AND_DISK_2)

    allGames
  }


  def createAllTables(games: DataFrame): DataFrame = {
    val sparkSession = SparkSession.builder()
      .getOrCreate()

    import sparkSession.implicits._

    val allDates = games.select($"date").distinct().rdd.map(r => r.getTimestamp(0)).collect()
    var allTables = createTable(games).withColumn("at_date", lit(allDates.last))
//    allDates.foreach(
//      date => allTables = allTables.union(createTable(games.where($"date" < date)).withColumn("at_date", lit(date)))
//    )

    allTables
  }

  def createTable(games: DataFrame) : DataFrame = {

    val sparkSession = SparkSession.builder()
      .getOrCreate()

    import sparkSession.implicits._

    val w = Window
      .orderBy($"points".desc)

    return games
      .groupBy("team")
      .agg(
        sum("points").as("points"),
        sum($"scored_goals" - $"conceded_goals").as("GD"),
        sum($"scored_goals").as("scored"),
        sum($"conceded_goals").as("conceded")
      )
      .withColumn("position", row_number().over(w))
      .select("position", "team", "points", "scored", "conceded", "GD")
      .persist(StorageLevels.MEMORY_AND_DISK_2)
  }
}
