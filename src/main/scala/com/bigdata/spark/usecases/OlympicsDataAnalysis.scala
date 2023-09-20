package com.bigdata.spark.usecases

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object OlympicsDataAnalysis {
  def main(args: Array[String]): Unit = {
    if (args.length == 3) {
      // step-1: SparkSession
      val ss = SparkSession.builder
        .master(args(0))
        .appName(args(1))
        .getOrCreate() // it creates spark-session and returns SparkSession object

      // step-2: Loading File
      ss.sparkContext.setLogLevel("WARN")
      val r1 = ss.sparkContext.textFile(args(2))

      // Display 10 records to check dataset
      //r1.take(10).foreach(println)

      // step-3: Processing

      val counts = r1.filter {
        x => {
          if (x.toString().split("\t").length >= 10)
            true
          else
            false
        }
      }.map(line => {
        line.toString().split("\t")
      })

      // Here filtering the data based on a condition that has tab-spaces
      // length >= 10 will lke sure that we are bringing records with all 10 columns
      // If the condition is true then we are reading that line
      // Here we can use () brackets instead of {} for filter, but we have implemented custom logic inside so recommended to use {} brackets

      // counts.take(10).foreach(println)
      // the output we get is array of strings

      val fil = counts.filter(
        x => {
          if (x(5).equalsIgnoreCase("swimming") && (x(9).matches("\\d+")))
            true
          else
            false
          // we are retrieving records with swimming and x(5) means 5th column starting with 0
          // we are retrieving records only with any value to sum for total number of medals in 9th column
        }
      )
      //fil.take(10).foreach(println)
      //println(fil.count())

      val pairs: RDD[(String, Int)] = fil.map(x => (x(2), x(9).toInt))
      // Here we are creating a pair RDD with country_name,count_medals
      val cnt = pairs.reduceByKey(_ + _)

      //cnt.take(10).foreach(println)

      // Get the Hadoop Configuration
      val hadoopConfig= ss.sparkContext.hadoopConfiguration

      // Specify the path of the folder you want to delete
      val folderPath = new Path("C:\\bigdata\\Workspace\\output_files\\sample2")

      // Initialize the Hadoop FileSystem
      val fs = FileSystem.get(hadoopConfig)

      // Check if the folder exists
      if (fs.exists(folderPath)) {
        // Delete the folder and its contents recursively
        fs.delete(folderPath, true)
        println(s"Folder $folderPath deleted successfully.")
      } else {
        println(s"Folder $folderPath does not exist.")
      }

      cnt.saveAsTextFile("C:\\bigdata\\Workspace\\output_files\\sample2")

    } else {
      //println(cnt)
      //cnt.take(10).foreach(println)
      println("please enter three args")
    }
  }
}

