/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.jmh

import java.lang.{Iterable => JIterable}
import java.util.concurrent.TimeUnit

import com.google.common.collect.Lists
import org.openjdk.jmh.annotations._

import scala.collection.JavaConverters._

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class JoinBenchmark {

  def genIterable(n: Int): JIterable[Int] = {
    val l = Lists.newArrayList[Int]()
    (1 to n).foreach(l.add)
    l
  }

  val i1 = genIterable(1)
  val i10 = genIterable(10)
  val i100 = genIterable(100)

  @Benchmark def forYieldA: Unit = forYield(i1, i10)
  @Benchmark def forYieldB: Unit = forYield(i10, i1)
  @Benchmark def forYieldC: Unit = forYield(i10, i100)
  @Benchmark def forYieldD: Unit = forYield(i100, i10)

  @Benchmark def artisanA: Unit = artisan(i1, i10)
  @Benchmark def artisanB: Unit = artisan(i10, i1)
  @Benchmark def artisanC: Unit = artisan(i10, i100)
  @Benchmark def artisanD: Unit = artisan(i100, i10)

  def forYield(as: JIterable[Int], bs: JIterable[Int]): Unit = {
    val xs: TraversableOnce[(Int, Int)] =
      for (a <- as.asScala.iterator; b <- bs.asScala.iterator) yield (a, b)
    val i = xs.toIterator
    while (i.hasNext) output(i.next())
  }

  def artisan(as: JIterable[Int], bs: JIterable[Int]): Unit = {
    (peak(as), peak(bs)) match {
      case ((1, a), (1, b)) => output(a, b)
      case ((1, a), (2, _)) =>
        val i = bs.iterator()
        while (i.hasNext) output(a, i.next())
      case ((2, _), (1, b)) =>
        val i = as.iterator()
        while (i.hasNext) output(i.next(), b)
      case ((2, _), (2, _)) =>
        val i = as.iterator()
        while (i.hasNext) {
          val j = bs.iterator()
          while (j.hasNext) {
            output(i.next(), j.next())
          }
        }
      case _ => ()
    }
  }

  @inline private def peak[A](xs: java.lang.Iterable[A]): (Int, A) = {
    val i = xs.iterator()
    if (i.hasNext) {
      val a = i.next()
      if (i.hasNext) (2, null.asInstanceOf[A]) else (1, a)
    } else {
      (0, null.asInstanceOf[A])
    }
  }

  private def output(x: (Int, Int)): Unit = Unit

}
