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

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferencePosition
import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.formats.avro.{ ADAMRecord, ADAMContig }
import org.bdgenomics.RNAdam.models.{ ReadPair }

class ClassifyTestSuite extends SparkFunSuite {

  sparkTest("put your test here") {
    val mySc = sc // this is your SparkContext

    val contig1 = ADAMContig.newBuilder
      .setContigName("chr1")
      .build

    val recFirst = ADAMRecord.newBuilder()
      .setReadName("foo")
      .setFirstOfPair(true)
      .setSecondOfPair(false)
      .setReadMapped(true)
      .setContig(contig1)
      .build()

    val recSecond = ADAMRecord.newBuilder()
      .setReadName("foo")
      .setFirstOfPair(false)
      .setSecondOfPair(true)
      .setReadMapped(true)
      .setContig(contig1)
      .build()

    val records = Seq(recFirst, recSecond)

    println("Records:")
    records.foreach(x => println(x))

    val recordsPaired: Seq[ReadPair] = Defuse.findReadPairs(records)

    println("Record pairs:")
    recordsPaired.foreach(x => println(x))

    val recordsRdd = sc.parallelize(records)

    val recordsGrouped: RDD[(String, Seq[ADAMRecord])] = Defuse.preClassify(recordsRdd)

    println("Record names:")
    recordsGrouped.foreach(x => println(x._1))
  }

}
