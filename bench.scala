package com.stripe.mapbench

import java.util.Arrays

import scala.collection.mutable.LongMap
import scala.util.Random

import com.twitter.algebird._

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Scope

object MapTraversal {

  @State(Scope.Benchmark)
  class Maps {
    val items: List[Int] = List.range(0, 4096)

    val immutableMap: Map[Int, Long] =
      Map(items.map { i => i -> i.toLong }: _*)

    val longMap: LongMap[Long] =
      LongMap(items.map { i => i.toLong -> i.toLong }: _*)

    val arrayMap: ArrayMap =
      ArrayMap(immutableMap.toList: _*)
  }
}

class MapTraversal {
  import MapTraversal._

  @Benchmark
  def immutableMap(maps: Maps): Long = {
    var i = 0L
    maps.immutableMap.foreach { case (key, value) =>
      i += key + value
    }
    i
  }

  @Benchmark
  def longMapForeach(maps: Maps): Long = {
    var i = 0L
    maps.longMap.foreach { case (key, value) =>
      i += key + value
    }
    i
  }

  @Benchmark
  def longMapForeachKey(maps: Maps): Long = {
    val m = maps.longMap
    var i = 0L
    m.foreachKey { key =>
      i += key + m(key)
    }
    i
  }

  @Benchmark
  def arrayMapForeach(maps: Maps): Long = {
    var i = 0L
    maps.arrayMap.foreach { (key, value) =>
      i += key + value
    }
    i
  }
}

object HLLSeriesBenchmark {

  @State(Scope.Benchmark)
  class Data {
    def int2bytes(n: Int): Array[Byte] = {
      val bytes = new Array[Byte](4)
      bytes(0) = n.toByte
      bytes(1) = (n >>> 8).toByte
      bytes(2) = (n >>> 16).toByte
      bytes(3) = (n >>> 24).toByte
      bytes
    }

    def genRandomData(seed: Int)(f: Random => Int): List[(Array[Byte], Long)] = {
      val rng = new Random(seed)
      List.fill(1000)(f(rng)).map(int2bytes).map(_ -> rng.nextLong)
    }

    val randomDenseData = genRandomData(23)(_.nextGaussian.toInt)
    val randomSparseData = genRandomData(91)(_.nextInt)
  }
}

class HLLSeriesBenchmark {
  import HLLSeriesBenchmark._

  def bits = 8

  @Benchmark
  def myHLLSeriesDense(data: Data): MyHLLSeries = {
    val hllSeriesMonoid = new MyHyperLogLogSeriesMonoid(bits)
    hllSeriesMonoid.sum(data.randomDenseData.map { case (bytes, ts) =>
      hllSeriesMonoid.create(bytes, ts)
    })
  }

  @Benchmark
  def myHLLSeriesSparse(data: Data): MyHLLSeries = {
    val hllSeriesMonoid = new MyHyperLogLogSeriesMonoid(bits)
    hllSeriesMonoid.sum(data.randomSparseData.map { case (bytes, ts) =>
      hllSeriesMonoid.create(bytes, ts)
    })
  }

  @Benchmark
  def hllSeriesDense(data: Data): HLLSeries = {
    val hllSeriesMonoid = new HyperLogLogSeriesMonoid(bits)
    hllSeriesMonoid.sum(data.randomDenseData.map { case (bytes, ts) =>
      hllSeriesMonoid.create(bytes, ts)
    })
  }

  @Benchmark
  def hllSeriesSparse(data: Data): HLLSeries = {
    val hllSeriesMonoid = new HyperLogLogSeriesMonoid(bits)
    hllSeriesMonoid.sum(data.randomSparseData.map { case (bytes, ts) =>
      hllSeriesMonoid.create(bytes, ts)
    })
  }
}
