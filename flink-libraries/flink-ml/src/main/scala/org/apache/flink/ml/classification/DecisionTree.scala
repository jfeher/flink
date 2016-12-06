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
  def gini(dataSet: DataSet[Tuple4[Boolean, Boolean, Boolean, Int]], column: Int): Double ={
    val counts = dataSet
      .map(x=>(x._1, x._2, x._3, 1))
      .groupBy(column, 2)
      .sum(3)

    val count = dataSet.count().toDouble
    val sum = dataSet
      .map(x=>(x._1, x._2, x._3, 1)).groupBy(column).sum(3)

    val res = counts.join(sum)
      .where(column)
      .equalTo(column)

    val giniTemp= res
          .map(x => (x._1._3, x._1._4.toDouble/x._2._4, x._2._4))
          .map(x => (x._1, x._2*x._2, x._3))
          .groupBy(0)
          .sum(1)
          .map(x => (x._1, (1 - x._2)* (x._3.toDouble/count)))

    val gini2= giniTemp
          .sum(1)
          .map(_._2)
          .collect().head

    gini2
  }

  def purity(dataSet: DataSet[Tuple4[Boolean, Boolean, Boolean, Int]], id: Int): Double ={
    val pur = dataSet.filter(_._4 == id)
                        .map(x => (x._3, 1))
                        .groupBy(0)
                        .sum(1)
                        .collect().head._2.toDouble
    val count = dataSet.filter(_._4 == id).count().toDouble
    pur/count
  }

  class Node(co: Int, c: List[Node], l: Boolean, i: Int, cl: Boolean, p: Double){
    var children: List[Node] = c
    var column: Int = co
    var label: Boolean = l
    val id: Int = i
    var classification: Boolean = cl
    val purity = p
  }

  def printTree(n: Node): Unit ={
    println(n.id, n.column, n.label, n.children.size, n.classification, n.purity)
    if(n.children.nonEmpty){
      n.children.foreach(x => printTree(x))
    }
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
      tempNode = newNode
    }
    tempNode.classification
  }

  def growTree(dataSet: DataSet[Tuple3[Boolean, Boolean, Boolean]]): Node ={
    // every datapoint starts in the node with id=1
    var dataWithID = dataSet.map(x => Tuple4(x._1, x._2, x._3, 1))
    var leaf = ListBuffer[Node]()
    val n = new Node(-1, List(), false, 1, false, -1.0)
    leaf += n
    var count=0
    var minCount=0
    var minPur=0.0
    var zeroCount = 0
    val purs = new Array[Double](2)
    // create new leaves until we still have inner nodes in the tree
    while(leaf.nonEmpty) {
      count+=1
      val tempNode = leaf.remove(0)
      var min = 1.0
      var minCol = -1
      val id = tempNode.id
      //check if the node should be a leaf
      val nodePur = purity(dataWithID, id)

      if(nodePur == 0 | nodePur == 1){
        // create the leaf
        val id = tempNode.id
        //        val label = dataWithID.filter( _._4 == id).first(1).collect()(0)._3
        // choose the class that appears more as the class of the leaf
        val label = dataWithID
          .filter(_._4 == id)
          .map(x => (x._1, x._2, x._3, x._4, 1))
          .groupBy(2)
          .reduce((x, y) => (x._1, x._2, x._3, x._4, x._5 + y._5))
          .reduce((x, y) => if (x._5 > y._5) {
            x
          } else {
            y
          })
          .collect().head._3
        tempNode.classification = label

      } else {
        // compute purity for cutting at each column
        for (i <- 0 to 1) {
          val id = tempNode.id
          val pur = gini(dataWithID.filter(_._4 == id), i)
//          purs(i) = pur
          if (min > pur) {
            min = pur
            minCol = i
          }
        }

        val id = tempNode.id
        minPur = min
        // create the new nodes if we haven't reached a leaf
//        if (min > 0.0) {
          minCount += 1
          val c1 = new Node(-1, List(), true, tempNode.id * 2, true, minPur)
          val c2 = new Node(-1, List(), false, tempNode.id * 2 + 1, false, minPur)

          var ctemp = Array() :+ c1
          ctemp = ctemp :+ c2
          tempNode.children = ctemp.toList
          leaf += c1
          leaf += c2
          tempNode.column = minCol

        // modify the id's of the datapoints
        dataWithID = dataWithID.map(x => if (x._4 == id) {
          if (x.productElement(minCol) == false) {
            (x._1, x._2, x._3, x._4 * 2 + 1)
          } else {
            (x._1, x._2, x._3, x._4 * 2)
          }
        } else {
          x
        })
      }
    }
    dataWithID.print()
    println("mincount", minCount, minPur, zeroCount)
    n
  }

}

object DecisionTree{

  // ========================================== Factory methods ====================================

  def apply(): DecisionTree = {
    new DecisionTree()
  }





}
