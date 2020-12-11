package com.netflix.atlas.core.util

import org.apache.datasketches.cpc.CpcSketch
import org.apache.datasketches.hll.HllSketch
import org.apache.datasketches.hll.TgtHllType

import scala.util.hashing.MurmurHash3

trait CardinalityEstimator {
  def update(obj: AnyRef): Unit
  def cardinality: Int
}

object CardinalityEstimator {

  val NumBucketsCustom = 9
  val NumBucketsHll = 9
  val NumBucketsCpc = 9

  def hyperLogLog(h: AnyRef => Int): CardinalityEstimator = {
    new HyperLogLog(h)
  }

  def hyperLogLogLibMM3: CardinalityEstimator = {
    hyperLogLog(r => org.apache.datasketches.hash.MurmurHash3.hash(Array(r.hashCode()), 9001L)(0).toInt)
  }

  def hyperLogLogScalaMM3: CardinalityEstimator = {
    hyperLogLog(r => MurmurHash3.arrayHash(Array(r.hashCode())))
  }

  // http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
  private class HyperLogLog(h: AnyRef => Int) extends CardinalityEstimator {

    private val buckets = NumBucketsCustom
    private val maskOfBucketBits = 0xFFFFFFFF >>> buckets

    require(buckets >= 4 && buckets <= 16)

    private val m = math.pow(2, buckets).toInt

    private val alpha = m match {
      case 16            => 0.673
      case 32            => 0.697
      case 64            => 0.709
      case x if x >= 128 => 0.7213 / (1 + 1.079 / m)
    }

    // registers
    private val M = new Array[Byte](m)

    // for large range check
    private val L = math.pow(2, 32)

    // cached estimate
    private var estimate = -1

    private def positionOfFirstOneBit(x: Int): Byte = {
      // Ignore bits used for address and then remove address offset - 1
      (Integer.numberOfLeadingZeros(x & maskOfBucketBits) - (buckets - 1)).toByte
    }

    override def update(obj: AnyRef): Unit = {
      val x = h(obj)
      val j = x >>> (32 - buckets)
      val p = positionOfFirstOneBit(x)
      if (p > M(j)) {
        M(j) = p
        estimate = -1
      }
    }

    private def computeCardinalityEstimate: Int = {
      var sum = 0.0
      var V = 0 // registers that are zero
      var i = 0
      while (i < m) {
        sum += math.pow(2.0, -M(i))
        if (M(i) == 0) {
          V += 1
        }
        i += 1
      }
      val E = ((alpha * m * m) / sum).toInt

      if (E < 2.5 * m && V != 0) {
        // small range correction
        (m * math.log(m / V)).toInt
      } else if (E > L / 30) {
        // large range correction
        (-L * math.log(1.0 - E / L)).toInt
      } else {
        E
      }
    }

    override def cardinality: Int = {
      if (estimate < 0) {
        estimate = computeCardinalityEstimate
      }
      estimate
    }
  }


  // Apache datasketch implementations

  def cpc: CardinalityEstimator = {
    new Cpc
  }

  private class Cpc extends CardinalityEstimator {
    private val sketch = new CpcSketch(NumBucketsCpc)

    override def update(obj: AnyRef): Unit = {
      sketch.update(obj.hashCode())
    }

    override def cardinality: Int = sketch.getEstimate.toInt
  }

  def cpcLowBias: CardinalityEstimator = {
    new CpcLowBias
  }

  private class CpcLowBias extends CardinalityEstimator {
    private val sketch = new CpcSketch(NumBucketsCpc)

    override def update(obj: AnyRef): Unit = {
      sketch.update(Hash.lowbias32(obj.hashCode()))
    }

    override def cardinality: Int = sketch.getEstimate.toInt

    override def toString: String = sketch.toString()
  }


  def hll: CardinalityEstimator = {
    new Hll
  }

  private class Hll extends CardinalityEstimator {
    private val sketch = new HllSketch(NumBucketsHll, TgtHllType.HLL_8)

    override def update(obj: AnyRef): Unit = {
      sketch.update(obj.hashCode())
    }

    override def cardinality: Int = sketch.getEstimate.toInt
  }
}

// #################################################################################################
