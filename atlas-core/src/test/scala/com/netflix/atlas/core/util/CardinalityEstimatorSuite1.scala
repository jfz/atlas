package com.netflix.atlas.core.util

import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.util.CardinalityEstimator._
import com.netflix.atlas.json.Json
import org.openjdk.jol.info.GraphLayout
import org.scalatest.funsuite.AnyFunSuite

//import java.util.UUID
import scala.util.Using

class CardinalityEstimatorSuite1 extends AnyFunSuite {

  // just to get warnings printed up front
  GraphLayout.parseInstance(this)

  private def check(name: String, estimator: CardinalityEstimator, values: Seq[AnyRef]): Unit = {
    println(s"checking $name")
    var errorSum = 0.0
    var lastErr = 0.0
    values.zipWithIndex.foreach {
      case (v, i) =>
        estimator.update(v)
        //if (i % 10 == 0) {
        val actual = i + 1
        val estimate = estimator.cardinality
//        println(f"   actual: $i%4d estimate: $estimate%4d")
        val percentError = 100.0 * math.abs(estimate - actual) / actual
        lastErr = percentError
        errorSum += percentError
//        println(s"actual = $actual, estimate = $estimate, percent error = $percentError")
      //}
    }
    val avgPctError = errorSum / values.size
    val footprint = GraphLayout.parseInstance(estimator).totalSize()
    println(f"$name%25s: $avgPctError%8.2f%% $footprint%8d bytes $lastErr%8.2f%%")
//    println(estimator)

  }

  private def check(name: String, values: Seq[String]): Unit = {

    println(name)
    println("-" * 51)
    check("custom default", hyperLogLog(_.hashCode()), values)
    check("custom lowbias", hyperLogLog(r => Hash.lowbias32(r.hashCode())), values)

    check("custom murmur3", hyperLogLogLibMM3, values)
    check("hll murmur3", hll, values)
    check("cpc murmur3", cpc, values)
    println()
  }

//  test("int strings") {
//    check("int strings", (0 until 100_000).map(_.toString))
//  }
//
//  test("words") {
//    import Streams._
//    val values = Using.resource(fileIn("/usr/share/dict/words")) { in =>
//      lines(in).toList
//    }
//    check("words", values)
//  }
//
//  test("uuids") {
//    val values = (0 until 100_000).map(_ => UUID.randomUUID().toString)
//    check("uuids 100k", values.slice(0,100_000))
//    check("uuids 10k", values.slice(0,10_000))
//    check("uuids 1k", values.slice(0,1_000))
//    check("uuids 100", values.slice(0,100))
//    check("uuids 10", values.slice(0,10))
//    check("uuids 1", values.slice(0,1))
//  }

  test("tsTagIds") {
    import Streams._
    val values = Using.resource(fileIn("/path/to/tags1M")) { in =>
      lines(in).toList
        .map(Json.decode[Map[String, String]](_))
        .map(tags => TaggedItem.computeId(tags).toString.substring(6))
    }

    values.slice(0,5).foreach(println); println()

//    check("tsTagIds 1M", values)
//    check("tsTagIds 500k", values.slice(0,1_000_000))
//    check("tsTagIds 100k", values.slice(0,100_000))
//    check("tsTagIds 10k", values.slice(0,10_000))
    check("tsTagIds 1k", values.slice(0,1_000))
//    check("tsTagIds 100", values.slice(0,100))
//    check("tsTagIds 10", values.slice(0,10))
//    check("tsTagIds 1", values.slice(0,1))
  }
}
