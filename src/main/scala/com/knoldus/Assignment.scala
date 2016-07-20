package com.knoldus

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import java.io.{File, PrintWriter}

/**
  * the class provide solution of spark assignment 1, answers to the questions are given
  * in coresponding methods
  *
  * @param inputFilePath path of the input file, the input file is added in the project resources
  */
class Assignment(inputFilePath:String) {

  //creating configuration and context of Spark
  val conf: SparkConf = new SparkConf().setAppName("spark_assignment_1").setMaster("local")
  val sc: SparkContext = new SparkContext(conf)

  //question 1
  val pageCount: RDD[String] = sc.textFile(inputFilePath)

  /**
    * this method answers question 2 of the assignment
    *
    * @param outputFilePath path of the output file, the output file is added
    *                       in the project resources as well
    * @return boolean value that indicates weather the process of writing in the output file
    *         completed with a success or a failure
    */
  def question2(outputFilePath:String):Boolean= {

    val tenRandomRecords: Array[String] = pageCount.takeSample(false, 10) //take 10 random records sample from all records without replacement
    try {
      val writer = new PrintWriter(new File(outputFilePath))
      tenRandomRecords.foreach(x => writer.write(x + "\n"))
      writer.close()
      true
    }
    catch {
      case e: Exception => false
    }
  }

  /**
    * this method answers question 3 of the assignment
    *
    * @return long value indicating total records in input file
    */
  def question3():Long={

    pageCount.count
  }

  /**
    * this method answers question 4 of the assignment,
    * the method is further used to answer question 5
    *
    * @param allRecordRdd RDD of all records of input file
    * @return RDD of records from input file having english as its language
    */
  private def question4(allRecordRdd:RDD[String]):RDD[String]={

    allRecordRdd.filter(x => Evaluate.findEnglish(x))
  }

  /**
    * this method answers question 4 of the assignment
    *
    * @return long value indicating total records of english language
    */
  def question5():Long={

    question4(pageCount).count
  }

  /**
    * this method answers question 4 of the assignment
    *
    * @param greaterThanValue long value on basis of which all records are filtered
    *                         (200,000 according to question)
    * @return pages requested more than greaterThanValue
    */
  def question6(greaterThanValue:Long):Long={

    val result: RDD[String] = pageCount.filter(x => Evaluate.compute(x,greaterThanValue))
    result.count
  }

}

/**
  * the singleton object facilitate call to a method in spark transformation
  */
object Evaluate{

  /**
    * the methods extract the value of the 'number of request' column and if it exist,
    * check weather its greater than greaterThanValue or not
    *
    * @param str single string record from the input file
    * @param greaterThanValue value on basis of which records are filtered
    * @return boolean value; true if record is greater, false otherwise.
    */
  def compute(str:String, greaterThanValue:Long):Boolean={

    val tempResult: Option[String] = Option(str.split(" ").apply(2)) // 2 is the column for number of record
    if(tempResult.isDefined) {
      val result: Long = tempResult.get.toLong
      result > greaterThanValue
    }
    else{false}
  }

  /**
    * the method checks weather the project code language is english or not
    *
    * @param str the input string record
    * @return boolean status telling weather the language is english or not
    */
  def findEnglish(str:String):Boolean={

    val tempResult: Option[String] = Option(str.split(" ").apply(0)) //0 is the index of first column ie project code
    tempResult.get == "en" //en is code for english language
  }

}
