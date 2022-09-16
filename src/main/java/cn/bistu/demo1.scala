package cn.bistu

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Log
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author : Alex 
 * @Description :
 */
object demo1 {
    def main(args: Array[String]): Unit = {
        
        
        val conf = new SparkConf().setMaster("local[*]").setAppName("demo1")
        val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        import ss.implicits._
    
        val path = "/Users/harakuro/Documents/codeRepo/sparkdemo/data/ratings.csv"
        
        val df: DataFrame = ss.read.format("csv")
            .option("sep",",")
            .option("inferSchema","true")
            .option("header","true")
            .load(path)
    
        
        
        
        
        ss.stop()
    }
}
