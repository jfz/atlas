package com.netflix.atlas.eval.model

import akka.util.ByteString
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonSubTypes, JsonTypeInfo}
import com.netflix.atlas.akka.ByteStringInputStream
import com.netflix.atlas.json.{Json, JsonSupport}

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")  //use property "type" for type mapping
@JsonSubTypes(Array(
  new Type(value = classOf[LwcHeartbeat], name = "heartbeat") //name of this type is "heartbeat"
))
abstract class LwcDataType extends JsonSupport {
  protected var `type` = "unknown"
}

object LwcDataType {
  def main(args: Array[String]): Unit = {
    val heartbeat = LwcHeartbeat(System.currentTimeMillis() / 5000 * 5000, 5000)
    val str = heartbeat.toJson
    println(str)

    val lwcData = Json.decode[LwcDataType](new ByteStringInputStream(ByteString(str)))
    println(lwcData)
  }
}

