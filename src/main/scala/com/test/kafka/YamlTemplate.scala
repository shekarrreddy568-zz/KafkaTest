package com.test.kafka

import java.util.Dictionary

import scala.beans.BeanProperty

class YamlTemplate {

  @BeanProperty var nameSpace: String = null
  @BeanProperty var topics = new java.util.ArrayList[Dictionary[String, Any]]()

  override def toString: String = {
    return s"$nameSpace, $topics"
  }
}
