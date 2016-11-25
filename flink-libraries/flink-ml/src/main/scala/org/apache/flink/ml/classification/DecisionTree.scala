/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.flink.ml.classification

import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer


class DecisionTree {
  def purity(dataSet: DataSet[Tuple4[Boolean, Boolean, Boolean, Int]], column: Int): Double ={
    val counts = dataSet
      .map(x=>(x._1, x._2, x._3, 1))
      .groupBy(column)
      .sum(3)

    val sum = counts.sum(3).first(1).collect().map(_._4).head
//    var sum = 0
//    sumOpt match{
//      case Some(x) => sum = x
//      case None => sum = -1
//    }
    val giniTemp= counts
      //.map(x => x._4)
      .map(x => (x._1, x._4.toDouble/sum))
      .map(x => (x._1, x._2*x._2))
      .sum(1)
      .map(_._2)
      .collect().head
    val gini = 1 - giniTemp
    println(gini)
    gini
  }
//  class Node(d: DataSet[Tuple3[Boolean, Boolean, Boolean]], co: Int,
//             c: List[Node], l: Boolean, i: Int){
//    //var data: DataSet[Tuple3[Boolean, Boolean, Boolean]] = d
//    var children: List[Node] = c
//    var column: Int = co
//    var label: Boolean = l
//    val id: Int = i
//  }

  class Node(co: Int, c: List[Node], l: Boolean, i: Int, cl: Boolean){
    //var data: DataSet[Tuple3[Boolean, Boolean, Boolean]] = d
    var children: List[Node] = c
    var column: Int = co
    var label: Boolean = l
    val id: Int = i
    var classification: Boolean = cl
  }

  def predict(input: Tuple2[Boolean, Boolean], node: Node): Boolean ={
    var tempNode = node
    var newNode = node
    while(tempNode.children.nonEmpty){
      val x = input.productElement(tempNode.column)

      for(child <- tempNode.children){
        if(child.label == x){
          newNode = child
        }
      }
      println("node", newNode.id)
      tempNode = newNode
    }
    tempNode.classification
  }

  def growTree(dataSet: DataSet[Tuple3[Boolean, Boolean, Boolean]]): Node ={
    val dataWithID = dataSet.map(x => Tuple4(x._1, x._2, x._3, 1))
    var done = ListBuffer[Node]()
    var leaf = ListBuffer[Node]()
    val n = new Node(-1, List(), false, 1, false)
    leaf += n
    while(leaf.nonEmpty) {
      val tempNode = leaf.remove(0)
      var min = 1.0
      var minCol = 1

      for (i <- 1 to 3) {
//            val pur = purity(tempNode.data, i)
        val id = tempNode.id
        val pur = purity(dataWithID.filter( _._4 == id), i)
        if (min > pur) {
          min = pur
          minCol = i
        }
      }
      if(min>0) {
//TODO correct classification label

        val c1 = new Node(-1, List(), true, tempNode.id * 2, true)
        val c2 = new Node(-1, List(), false, tempNode.id * 2 + 1, false)

        var ctemp = Array() :+ c1
        ctemp = ctemp :+ c2
        tempNode.children = ctemp.toList
        leaf += c1
        leaf += c2
        tempNode.column = minCol
      }
      else {
        val id = tempNode.id
        val label = dataWithID.filter( _._4 == id).first(1).collect()(0)._3
        tempNode.label = label
      }
    }

    println(n.children.length)
    n
  }

}

object DecisionTree{

  val data = Array(("blue", "large", true), ("red", "large", true),
    ("blue", "small", false), ("red", "small", false))



  // ========================================== Factory methods ====================================

  def apply(): DecisionTree = {
    new DecisionTree()
  }





}
