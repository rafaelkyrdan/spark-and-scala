import scala.collection.mutable.{Map => MMap}

/**
  * TODO:
  * 1. put on github
  * 2. add readme
  * 3. try to re-write count method with pure functions
  * 4. try to find better criteria to stop loop
  * 5. try to use Graph entity
  *
  */

object Parse {

  def main(args: Array[String]) {
    val ls = parseString("1-2,1-4,2-4,4-5,5-6,6-1,8-9,10")
    val nodes = parseNodes(ls)
    println(count(nodes))
  }

  def parseString(s:String) = {
    s.split(",").map({ case (x: String) => x.split("-") })
  }

  def parseNodes(ls:Array[Array[String]]):MMap[Int,Node] = {

    @annotation.tailrec
    def go(ls:List[Array[String]], m:MMap[Int, Node]):MMap[Int, Node] = {
      ls match {
        case h::t => {
          val l = h.toList
          val n = m.getOrElse(l.head.toInt, createNode(l.head.toInt))
          if (l.size > 1) {
            val n1 = m.getOrElse(l.last.toInt, createNode(l.last.toInt))
            n.add(l.last.toInt)
            m.+=(l.last.toInt -> n1)
          }
          m.+=(l.head.toInt -> n)
          go(t, m)
        }
        case Nil => m
      }
    }
    go(ls.toList, MMap[Int, Node]())
  }

  def createNode(x:Int): Node = {
    new Node(x)
  }

  def count(m:MMap[Int, Node]):Int = {

    var graphId = 0
    var marked = 0
    val keys = m.keySet.toList
    val iter = m.iterator

    def mark(id:Int, graphId:Int):Unit={
      m.get(id) match {
        case None => // never happened because
        case Some(x) =>
          if (x.graphId == 0) {
            x.graphId = graphId
            marked += 1
            x.refs match {
              case Nil => //println("empty2")
              case x1::_ => x.refs.foreach((y:Int) => mark(y, graphId))
            }
          }
      }
    }

    //criteria to stop looping
    while(keys.size > marked){
      val p = iter.next()
      if (p._2.graphId == 0){
        graphId = graphId + 1
        mark(p._1, graphId)
      }
    }

    //uncomment to see the nodes
    //m.foreach(x => {println(x._2)})
    graphId
  }

  class Node(val n:Int){
    var refs:List[Int] = List()
    var graphId = 0

    def add(x:Int) = {
      this.refs = x::refs
    }

    def apply(n: Int): Node = {
      var id = n
      new Node(n)
    }

    override def toString = s"Node(id=$n, graphId=$graphId, references=$refs)"
  }

  class Graph{
    var nodes:Set[Node] = Set()

    def apply: Graph = new Graph()

    def add(n:Node) = {
      this.nodes += n
    }

    override def toString = s"Graph($nodes)"
  }
}



