package com.datareducer

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode

object DataReducer {
  def main(args: Array[String]): Unit = {

    // Path to json data
    /*var input = new String;
    var output = new String;
    if (args.length < 2 ) {
      input = "/Users/ydatta/Documents/Workspace/data/new_data/tmp/grocery-1.json"

      output = "grocery"
    }
    else {
      input = args(0)
      output = args(1)
    }*/

    val sparkConf = new SparkConf()
      .setAppName("Reducer")
      .setMaster("local[*]")
      .set("spark.executor.memory", "6g")
      .set("spark.ui.port", "8099")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    /*val jsonData = sqlContext.read.json(input).coalesce(1)
    // Print shcema
    jsonData.printSchema

    //register as temp table
    // jsonData.registerTempTable("jsondata")

    val jsonRDD = jsonData.toJSON
    val jsonObjectRDD = jsonRDD.map(content => JsonObject.fromJson(content))
    val jsonDocumentRDD = jsonObjectRDD.map(obj => JsonDocument.create(obj.get("unique_id").toString, obj))

    //val c = jsonDocumentRDD.take(20)
    jsonDocumentRDD.saveToCouchbase(output)

    val data = sc
      .parallelize(Seq("user_10002", "id2", "id3"))
      .couchbaseGet[JsonDocument]()
      .collect()*/


    //val dir = "/mnt/data/hcube"
    val dir = "/Users/ydatta/Documents/Workspace/code/reducer"
    val inputDir = s"$dir/schema"//dir + "/300/schema"

    val outputDir = s"$dir/output"//dir + "/300/reduced"
    val tableName = "300_schema"

    val df = sqlContext.read.option("mergeSchema", "true").parquet(inputDir)
    df.registerTempTable(tableName)

    val exclude = List("timestamp", "hcube_hour")
    val columns = df.columns.toList.filter(!exclude.contains(_))

    val days = sqlContext.sql("select distinct hcube_day from " + tableName + " order by hcube_day").rdd.map(_(0))
    // days.partitions.length

    val frames = for (day <- days.collect) yield {
      val data = sqlContext.sql("select * from " + tableName + " where hcube_day = " + day)
      val dayTableName = tableName + "_" + day
      data.registerTempTable(dayTableName)
      val q = "select distinct " + columns.mkString(", ") + " from " + dayTableName
      sqlContext.sql(q)
    }

    val resultDf = frames.reduce((a, b) => a.unionAll(b))
    
    resultDf.coalesce(2).write.mode(SaveMode.Append).partitionBy("hcube_day").save(outputDir)

  }
}
