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

package com.spotify.scio.util

import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.join.{CoGbkResult, CoGroupByKey, KeyedPCollectionTuple}
import org.apache.beam.sdk.values.{KV, TupleTag}

import scala.reflect.ClassTag

object ArtisanMultiJoin {

  def peak[A](xs: java.lang.Iterable[A]): Option[A] = {
    val i = xs.iterator()
    if (i.hasNext) {
      val a = i.next()
      if (i.hasNext) None else Some(a)
    } else {
      None
    }
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag](name: String,
                                                     a: SCollection[(KEY, A)],
                                                     b: SCollection[(KEY, B)])
  : SCollection[(KEY, (A, B))] = {
    val (tagA, tagB) = (new TupleTag[A](), new TupleTag[B]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .apply(s"CoGroupByKey@$name", CoGroupByKey.create())

    type DF = DoFn[KV[KEY, CoGbkResult], (KEY, (A, B))]
    a.context.wrap(keyed).withName("Artisan" + name)
      .applyTransform(ParDo.of(new DF {
        @ProcessElement
        private[util] def processElement(c: DF#ProcessContext): Unit = {
          val kv = c.element()
          val key = kv.getKey
          val result = kv.getValue
          val as = result.getAll(tagA)
          val bs = result.getAll(tagB)
          (peak(as), peak(bs)) match {
            case (Some(a), Some(b)) => c.output((key, (a, b)))
            case (Some(a), None) =>
              val i = bs.iterator()
              while (i.hasNext) c.output((key, (a, i.next())))
            case (None, Some(b)) =>
              val i = as.iterator()
              while (i.hasNext) c.output((key, (i.next(), b)))
            case _ => ()
          }
        }
      }))
  }

}
