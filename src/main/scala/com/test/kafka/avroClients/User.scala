package com.test.kafka.avroClients

import scala.annotation.switch

case class User(var id: Int, var event_date: Long, var tour_value: Double, var id_driver: Int, var id_passenger: Int) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this(0, 0L, 0.0, 0, 0)
  def get(field$: Int): AnyRef = {
    (field$: @switch) match {
      case 0 => {
        id
        }.asInstanceOf[AnyRef]
      case 1 => {
        event_date
        }.asInstanceOf[AnyRef]
      case 2 => {
        tour_value
        }.asInstanceOf[AnyRef]
      case 3 => {
        id_driver
        }.asInstanceOf[AnyRef]
      case 4 => {
        id_passenger
        }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }
  def put(field$: Int, value: Any): Unit = {
    (field$: @switch) match {
      case 0 => this.id = {
        value
        }.asInstanceOf[Int]
      case 1 => this.event_date = {
        value
        }.asInstanceOf[Long]
      case 2 => this.tour_value = {
        value
        }.asInstanceOf[Double]
      case 3 => this.id_driver = {
        value
        }.asInstanceOf[Int]
      case 4 => this.id_passenger = {
        value
        }.asInstanceOf[Int]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = User.SCHEMA$

}

object User {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"com.test.kafka.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"event_date\",\"type\":\"long\"},{\"name\":\"tour_value\",\"type\":\"double\"},{\"name\":\"id_driver\",\"type\":\"int\"},{\"name\":\"id_passenger\",\"type\":\"int\"}]}")
}