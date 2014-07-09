/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.RNAdam.algorithms.defuse

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.bdgenomics.RNAdam.models.{ApproximateFusionEvent, FusionEvent, ReadPair}
import org.bdgenomics.formats.avro.ADAMRecord

object Defuse {
  def run(records: RDD[ADAMRecord],
    alpha: Double): RDD[FusionEvent] = {
    val (concordant, spanning, split) = classify(records)
    val (lmin, lmax) = findPercentiles(concordant, alpha)
    val graph = buildGraph(spanning, lmax)
    val fusions = bestFusions(graph)
    val splitRecordToFusion = assignSplitsToFusions(fusions, split, lmin, lmax)
    val exactBoundary = findExactBoundaryForFusions(splitRecordToFusion)
    trueFusions(graph, exactBoundary)
  }

  /**
   * This will classify the Record into Concordant, Spanning, and Split Reads.
   *
   * From the deFuse paper:
   *   - `Concordant Read`: both ends have the same Contig name.
   *   - `Spanning Read`: both ends are mapped, but do not have the same Contig name.
   *   - `Split Read`: one end is mapped but the other is not mapped.
   * @param records These are the records to be bucketed and categorized.
   * @return        (Concordant Reads, Spanning Reads, Split Reads)
   * @author anitacita99
   *         dcunningham
   */
  def classify(records: RDD[ADAMRecord]): (RDD[ReadPair], RDD[ReadPair], RDD[ReadPair]) =
  {
    val mappedRecords = records.map(x => x.getReadName())
    var concordant: RDD[ReadPair] = Seq();
    var spanning: RDD[ReadPair] = List();
    var split: RDD[ReadPair] = List();
    for ((k: String, v: List[ADAMRecord]) <- mappedRecords) {
      if (v.count == 2) {
        val pair = ReadPair(v(0), v(1))
        if (sameTranscript(pair.first, pair.second)) {
          concordant += pair
        }
        else if (!hasTranscriptName(pair.second)) {
          split += pair
        }
        else {
          spanning += pair
        }
      }
    }
    (concordant, spanning, split)
  }

  def sameTranscript(first: ADAMRecord, second: ADAMRecord) {
    val firstContig = first.getContig()
    val secondContig = second.getContig()
    if (!hasTranscriptName(firstContig) || !hasTranscriptName(secondContig))
      return false
    first.getContig.getContigName.equals(second.getContig.getContigname)
  }

  def hasTranscriptName(record: ADAMRecord) {
    record.getContig() != null
  }

  /**
   * Calculates a fragment length distribution, and excludes outliers given an
   * alpha parameter.
   *
   * @param concordantRecords An RDD of ADAM reads.
   * @param alpha The top/bottom % of reads to exclude.
   * @return (l_{min}, l_{max}): Return the min and max length.
   */
  def findPercentiles(concordantRecords: RDD[ReadPair], alpha: Double): (Long, Long) =
    FragmentLengthDistribution.findPercentiles(concordantRecords, alpha)

  /**
   * This will construct the graph which contains the read pairs and the approximate fusion event.
   * @param spanningRecords These are the pairs of values which are spanning reads.
   * @param lmax            This is the maximum considered length for the insert
   * @author fnothaft
   */
  def buildGraph(spanningRecords: RDD[ReadPair], lmax: Long): RDD[(ApproximateFusionEvent, Seq[ReadPair])] =
    ???

  /**
   * Calculate the fusions which are considered to have the most support from the Spanning Reads.
   * @param graph A graph of the Spanning Reads and the Fusion Events
   * @return The fusion events which have the most support
   * @author tdanford
   */
  def bestFusions(graph: RDD[(ApproximateFusionEvent, Seq[ReadPair])]): RDD[ApproximateFusionEvent] =
    ???

  /**
   * @param fusions
   * @param splitRecords
   * @param lmin
   * @param lmax
   * @return
   * @author carlyeks
   */
  def assignSplitsToFusions(fusions: RDD[ApproximateFusionEvent], splitRecords: RDD[ReadPair], lmin: Long, lmax: Long): RDD[(ApproximateFusionEvent, ReadPair)] =
    ???

  def findExactBoundaryForFusions(splitRecordToFusions: RDD[(ApproximateFusionEvent, ReadPair)]): RDD[(ApproximateFusionEvent, FusionEvent)] =
    ???

  def trueFusions(graph: RDD[(ApproximateFusionEvent, Seq[ReadPair])], exactFusions: RDD[(ApproximateFusionEvent, FusionEvent)]): RDD[FusionEvent] =
    ???
}
