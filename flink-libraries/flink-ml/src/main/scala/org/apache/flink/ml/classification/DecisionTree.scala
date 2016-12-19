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
  // gini(parent) = 1 - Sum pi * pi
  // gini(children) = fi * gini(i)
  def gini(dataSet: DataSet[Tuple4[Boolean, Boolean, Boolean, Int]], column: Int): Double ={
    // count the number of records by the splitting of the column and the class label
    val counts = dataSet
      .map(x=>(x._1, x._2, x._3, 1))
      .groupBy(column, 2)
      .sum(3)

    // the number of all the records
    val count = dataSet.count().toDouble

    // compute for each value from the column where we want to make the split the number of records
    val sum = dataSet
      .map(x=>(x._1, x._2, x._3, 1)).groupBy(column).sum(3)

    //pair each count by the column and class label with their count by the column
    val res = counts.join(sum)
      .where(column)
      .equalTo(column)

    // compute the gini for each child and weigh them by the nimber of records in the child node
    val giniTemp= res
          .map(x => (x._1._3, x._1._4.toDouble/x._2._4, x._2._4))
          .map(x => (x._1, x._2*x._2, x._3))
          .groupBy(0)
          .sum(1)
          .map(x => (x._1, (1 - x._2)* (x._3.toDouble/count)))

    // sum and collect the gini of the children
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
  def growTreeIter(dataSet: DataSet[Tuple3[Boolean, Boolean, Boolean]], columns: Int): Node ={
    val columns = 2
    val data = Array(Tuple3(1, List[Boolean](true, true), true), Tuple3(2, List[Boolean](true, false), true),
      Tuple3(2, List[Boolean](true, false), true),
      Tuple3(1, List[Boolean](false, false), false), Tuple3(1, List[Boolean](true, true), false),
      Tuple3(3, List[Boolean](false, true), false), Tuple3(3, List[Boolean](false, false), false))
    val env = ExecutionEnvironment.getExecutionEnvironment
    val init = env.fromCollection(data)
    //    val sol = Array(Tuple2(8, 0))
    //    val sol = Array(new Node(-1, List(), false, 1, false, -1.0))
    val sol = Array(Tuple2(0, new Node(-1, List(), false, 1, false, -1.0)))
    val initSol = env.fromCollection(sol)
    val iter = initSol.iterateDelta(init, 1, Array(0)){
      (solution, workSet) =>

        val purWorkSet = workSet.map(x => (x._2(0), x._2(1), x._3.asInstanceOf[Boolean], x._1))
        val zeroes = purWorkSet
          .filter(_._3 != true)
          .distinct(3)
          .map(x => (x._4, 0))

        val countTemp = purWorkSet
          .filter(_._3 == true)
          .map(x => (x._4, 1))
          .union(zeroes)
          .groupBy(0)
          .sum(1)
          .map(x => (x._1, x._2))

        //the number of datapoints by node
        val sumTemp = purWorkSet
          .map(x => (x._4, 1))
          .groupBy(0)
          .sum(1)
          .map(x => (x._1, x._2))

        // the purity (number of true labels/number of datapoints) by node
        val purity = countTemp
          .join(sumTemp)
          .where(0)
          .equalTo(0)
          .map(x => (x._1._1, x._1._2.toDouble/x._2._2.toDouble))

        // the new items in the solution set: the datapoints that are in pure nodes
        val delta = purWorkSet
          .join(purity)
          .where(3)
          .equalTo(0)
          .filter(x => x._2._2 == 0 || x._2._2 == 1)
          .map(x => Tuple2(x._1._4, new Node(-1, List(), false, x._1._4, false, -1.0)))

        // the new workSet is the datapoints that are not in pure nodes
        val newWorkTemp = purWorkSet
          .join(purity)
          .where(3)
          .equalTo(0)
          .filter(x => x._2._2 > 0 && x._2._2 < 1)
          .map(x => (x._1._4, List(x._1._1, x._1._2), x._1._3))


        val newWorkSet = newWorkTemp.map{
          x =>
            val label = x._3
            x._2.map{
              case y: Boolean =>
                if (y && label) List[Tuple2[Int, Int]]((1, 0), (0, 0))
                if (y && !label) List[Tuple2[Int, Int]]((0, 1), (0, 0))
                if (!y && label) List[Tuple2[Int, Int]]((0, 0), (1, 0))
                if (!y && !label) List[Tuple2[Int, Int]]((0, 0), (0, 1))
            }.asInstanceOf[List[List[Tuple2[Int, Int]]]]
        }.reduce((x,y) => x.zip(y).map(x => x._1.zip(x._2).map(x => (x._1._1 + x._2._1, x._1._2 + x._2._2))))

        //        val newWorkSet = workSet

        (delta, newWorkSet)

    }

    iter.print()
    iter.first(1).collect().head._2

  }
  def growTree(dataSet: DataSet[Tuple3[Boolean, Boolean, Boolean]], columns: Int): Node ={
    // every datapoint starts in the node with id=1
    var dataWithID = dataSet.map(x => Tuple4(x._1, x._2, x._3, 1))
    var leaf = ListBuffer[Node]()
    val n = new Node(-1, List(), false, 1, false, -1.0)
    leaf += n
    var minPur=0.0
    // create new leaves until we still have inner nodes in the tree
    while(leaf.nonEmpty) {
      val tempNode = leaf.remove(0)
      var min = 1.0
      var minCol = -1
      val id = tempNode.id
      //check if the node should be a leaf
      val nodePur = purity(dataWithID, id)

      if(nodePur == 0 | nodePur == 1){
        // create the leaf
        val id = tempNode.id
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
        for (i <- 0 to columns - 1) {
          val id = tempNode.id
          val idIndex = columns + 1
          val pur = gini(dataWithID.filter(_.productElement(idIndex) == id), i)
          if (min > pur) {
            min = pur
            minCol = i
          }
        }

        val id = tempNode.id
        minPur = min
        // create the new nodes if we haven't reached a leaf
          val c1 = new Node(-1, List(), true, tempNode.id * 2, true, min)
          val c2 = new Node(-1, List(), false, tempNode.id * 2 + 1, false, min)

          var ctemp = Array() :+ c1
          ctemp = ctemp :+ c2
          tempNode.children = ctemp.toList
          leaf += c1
          leaf += c2
          tempNode.column = minCol

        val idIndex = columns + 1
        // modify the id's of the datapoints
        dataWithID = dataWithID.map(x => if (x.productElement(idIndex) == id) {
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
    n
  }

}

object DecisionTree{

  // ========================================== Factory methods ====================================

  def apply(): DecisionTree = {
    new DecisionTree()
  }





}
