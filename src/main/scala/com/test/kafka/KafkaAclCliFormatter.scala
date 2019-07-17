package com.test.kafka

import util.control.Breaks._

object KafkaAclCliFormatter {

    def main(args: Array[String]): Unit = {

      def CliFormatter() = {
        //val input = scala.io.StdIn.readLine()
        for (input <- scala.io.Source.stdin.getLines) {

          if (input.startsWith("{"))
            input.drop(1).split("\\b(\\w*CN\\w*)\\b").filter(! _.isEmpty).map(s => "CN" + s).foreach(x => println(Console.BLUE + x + "\n"))
          else println(Console.WHITE + input)
        }
        break()
      }
      CliFormatter()
    }

  }
