package com.test.kafka

import com.typesafe.scalalogging.LazyLogging

object DirectoryDependencyAnalysis extends LazyLogging {

  /** This Object consists of a main method which takes
    * input data of type array of strings and print the parent dependency
    * between the directories and the root directory
    */

  def main(args: Array[String]): Unit = {
    logger.info("starting directory dependency analysis...")

    val input = Array("/b/a,c", "/b/a/c,d", "/f/g/h/q,l", "p/o/i/u/z,m")

    try {
      input.map(element => element.split(","))        // split each element at ","
        .map(tree => (tree(0).split("/"), tree(1)))   // split the tree(first element) at "/" and create a tuple with the second element
        .map(tuple => tuple._2 +: tuple._1)                   // Prepend the root to the array of directories
        .map(arr => arr.filter(char => char != ""))           // filter the emplty strings
        .foreach { arr =>
        for (index <- 0 until arr.length - 1) {
          println(s"parent of ${arr(index)} is ${arr(index + 1)}")  // print the result
        }
        println(s"${arr.head} is root\n")                           // print the result

      }
    }
    catch {
      case e: Exception => logger.info(e.getMessage)
    }
  }


}
