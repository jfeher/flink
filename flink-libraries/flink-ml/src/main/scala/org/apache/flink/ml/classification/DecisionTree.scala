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
  def purity(dataSet: DataSet[Tuple3[Boolean, Boolean, Boolean]], column: Int): Double ={
    val counts = dataSet
      .map(x=>(x._1, x._2, x._3, 1))
      .groupBy(column)
      .sum(3)

    val sum = counts.sum(3).first(1).collect().map(_._4).head

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
  class Node(d: DataSet[Tuple3[Boolean, Boolean, Boolean]], co: Int,
             c: List[Node], l: Boolean, i: Int){
    var data: DataSet[Tuple3[Boolean, Boolean, Boolean]] = d
    var children: List[Node] = c
    var column: Int = co
    var label: Boolean = l
    val id:Int = i
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
    tempNode.data.first(1).collect().head._3
  }
  def growTree(dataSet: DataSet[Tuple3[Boolean, Boolean, Boolean]]): Node ={
    var done = ListBuffer[Node]()
    var leaf = ListBuffer[Node]()
    val n=new Node(dataSet, -1, List(), false, 1)
    leaf += n
    while(leaf.nonEmpty) {
      var tempNode = leaf.remove(0)

      var min = 1.0
      var minCol = 1
      for (i <- Range(1, 3)) {
        val pur = purity(tempNode.data, i)
        if (min > pur) {
          min = pur
          minCol = i
        }
      }
      if(min>0) {
        val child1 = dataSet.filter(_.productElement(minCol).equals(true))
        val child2 = dataSet.filter(_.productElement(minCol).equals(false))

        val c1 = new Node(child1, -1, List(), true, tempNode.id * 2)
        val c2 = new Node(child2, -1, List(), false, tempNode.id * 2 + 1)

        var ctemp = Array() :+ c1
        ctemp = ctemp :+ c2
        tempNode.children = ctemp.toList
        leaf += c1
        leaf += c2
        tempNode.column = minCol
//        println(min, minCol, n.children.length)
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
