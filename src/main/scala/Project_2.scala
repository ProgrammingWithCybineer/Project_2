import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import java.util.Scanner
import java.sql.DriverManager
import java.sql.Connection
import com.mysql.cj.xdevapi.UpdateStatement
import java.io.File
import java.io.PrintWriter





object Project_2 {
    def main(args: Array[String]): Unit={

   //Logger.getLogger("org").setLevel(level.Error)
       
//Loading CSV file and creating DataFrame
 def covidData():Unit={
                   val spark=SparkSession
                  .builder
                  .appName("sparkSQL")
                  .master("local[*]")
                  .getOrCreate()
                  
                    val csvFile =spark.read.format("csv")
                    .option("mode", "FAILFAST")
                    .option("inferSchema", "true")
                    .option("header", "true")
                    .load("input/covid-data.csv")
                     csvFile.printSchema()
                    csvFile.createOrReplaceTempView("temp_data")
                     
                        
 }                   
                    

                    

                    // Query for total number of shark attacks since certain date
                def method1():Unit={
                        println("Title of Query")
                        val result = spark.sql("select  (max(total_deaths)-min(total_deaths)) as DifferenceInDeath from temp_data")
                        result.show(160)
                        Thread.sleep(100000)
                        result.write.mode("overwrite").csv("results/")
                        
                }
             def method2(){
                      println("Title of Query")
                        val result = spark.sql("")
                        result.show(160)
                        Thread.sleep(100000)
                        result.write.mode("overwrite").csv("results/")
                    }
            def method3():Unit={
                        println("Title of Query")
                        val result = spark.sql("")
                        result.show(160)
                        Thread.sleep(100000)
                        result.write.mode("overwrite").csv("results/")
                    }
             def method4():Unit={
                        println("Title of Query")
                        val result = spark.sql("")
                        result.show(160)
                        Thread.sleep(100000)
                        result.write.mode("overwrite").csv("results/")
                    }  
             def method5():Unit={
                        println("Title of Query")
                        val result = spark.sql("")
                        result.show(160)
                        Thread.sleep(100000)
                        result.write.mode("overwrite").csv("results/")
                    } 
             def method6():Unit={
                        println("Title of Query")
                        val result = spark.sql("")
                        result.show(160)
                        Thread.sleep(100000)
                        result.write.mode("overwrite").csv("results/")
                    } 
             def method7():Unit={
                        println("Title of Query")
                        val result = spark.sql("")
                        result.show(160)
                        Thread.sleep(100000)
                        result.write.mode("overwrite").csv("results/")
                    } 
             def method8():Unit={
                        println("Title of Query")
                        val result = spark.sql("")
                        result.show(160)
                        Thread.sleep(100000)
                        result.write.mode("overwrite").csv("results/")
                    } 
             def method9():Unit={
                        println("Title of Query")
                        val result = spark.sql(" ")
                        result.show(160)
                        Thread.sleep(100000)
                        result.write.mode("overwrite").csv("results/")
                    } 
             def method10():Unit={
                        println("Title of Query")
                        val result = spark.sql("")
                        result.show(160)
                        Thread.sleep(100000)
                        result.write.mode("overwrite").csv("results/")
                    } 


                        spark.stop()
                    }




    }
  

