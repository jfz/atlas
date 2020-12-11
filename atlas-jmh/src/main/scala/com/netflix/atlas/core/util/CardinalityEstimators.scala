package com.netflix.atlas.core.util

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole


/**
  * jmh:run -wi 10 -i 10 -f1 -t1 -prof gc .*CardinalityEstimators.*
  *
  *
  *
[info] Benchmark                              Mode  Cnt          Score         Error  Units
[info] CardinalityEstimators.cpc             thrpt   10  106990165.930 ± 1337498.583  ops/s
[info] CardinalityEstimators.customDefault   thrpt   10  144441234.478 ± 1111959.876  ops/s
[info] CardinalityEstimators.customLibMM3    thrpt   10   88826635.068 ±  846296.086  ops/s
[info] CardinalityEstimators.customLowBias   thrpt   10  111688728.850 ± 1394148.888  ops/s
[info] CardinalityEstimators.customScalaMM3  thrpt   10   94038827.955 ±  948591.033  ops/s
[info] CardinalityEstimators.hll             thrpt   10   77278281.013 ±  800222.600  ops/s
  */
@State(Scope.Thread)
class CardinalityEstimators {

  private val N_MASK = 1024 * 1024 - 1
  private val N = N_MASK + 1
  private val strings = (0 until N).map(_.toString).toArray
  private var i = 0

  private val custom = CardinalityEstimator.hyperLogLog(_.hashCode())
  private val customLowBias = CardinalityEstimator.hyperLogLog(r => Hash.lowbias32(r.hashCode()))
  private val customLibMM3 = CardinalityEstimator.hyperLogLogLibMM3
  private val customScalaMM3 = CardinalityEstimator.hyperLogLogScalaMM3
  private val cpc = CardinalityEstimator.cpc
  private val hll = CardinalityEstimator.hll

  def update(estimator: CardinalityEstimator, bh: Blackhole): Unit = {
    estimator.update(strings(i))
    i = (i + 1) & N_MASK
    // assume we'll need to check the estimate to see if a rollup is needed after every update
    bh.consume(estimator.cardinality)
  }

  @Benchmark
  def customDefault(bh: Blackhole): Unit = {
    update(custom, bh)
  }

  @Benchmark
  def customLowBias(bh: Blackhole): Unit = {
    update(customLowBias, bh)
  }

  @Benchmark
  def customScalaMM3(bh: Blackhole): Unit = {
    update(customScalaMM3, bh)
  }

  @Benchmark
  def customLibMM3(bh: Blackhole): Unit = {
    update(customLibMM3, bh)
  }

  @Benchmark
  def cpc(bh: Blackhole): Unit = {
    update(cpc, bh)
  }

  @Benchmark
  def hll(bh: Blackhole): Unit = {
    update(hll, bh)
  }

}
