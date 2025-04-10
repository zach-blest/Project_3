package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main{
  //this helps to suppress the logs on my local machine
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    //initialize all vertices as undecided 0
    var g = g_in.mapVertices((id, _) => 0)
    var remaining_vertices = g.vertices.filter(_._2 == 0).count()
    var iteration = 0
    
    println("Initial active vertices: " + remaining_vertices)
    
    while (remaining_vertices > 0) {
      iteration += 1
      
      //assign random probabilities to undecided vertices
      val g_with_random = g.mapVertices((id, attr) => 
        if (attr == 0) (attr, scala.util.Random.nextDouble()) else (attr, 0.0)
      )
      
      //find vertices with higher values than all their neighbors
      val msg = g_with_random.aggregateMessages[(Int, Double)](
        triplet => {
          // Only consider undecided vertices
          if (triplet.srcAttr._1 == 0 && triplet.dstAttr._1 == 0) {
            // Send messages to determine if a vertex has higher value than its neighbors
            if (triplet.srcAttr._2 > triplet.dstAttr._2) {
              triplet.sendToDst((1, triplet.srcAttr._2))
            } else if (triplet.srcAttr._2 < triplet.dstAttr._2) {
              triplet.sendToSrc((1, triplet.dstAttr._2))
            } else {
              //if equal use vertex ID as tiebreaker 
              if (triplet.srcId < triplet.dstId) {
                triplet.sendToDst((1, triplet.srcAttr._2))
              } else {
                triplet.sendToSrc((1, triplet.dstAttr._2))
              }
            }
          }
        },
        (a, b) => if (a._2 > b._2) a else b
      )
      
      //vertices to add to MIS are those that are undecided and haven't received a message
      val vertices_to_add = g_with_random.vertices
        .leftJoin(msg) {
          case (vid, (status, prob), opt) =>
            if (status == 0 && opt.isEmpty) 1 
            else status  
        }
      
      //update the graph with new MIS vertices
      var g_updated = Graph(vertices_to_add, g.edges)
      
      //find neighbors of MIS vertices and mark them as excluded 
      val excluded_neighbors = g_updated.aggregateMessages[Int](
        triplet => {
          if (triplet.srcAttr == 1 && triplet.dstAttr == 0) {
            triplet.sendToDst(-1)
          }
          if (triplet.dstAttr == 1 && triplet.srcAttr == 0) {
            triplet.sendToSrc(-1)
          }
        },
        (a, b) => -1
      )
      

      g = g_updated.joinVertices(excluded_neighbors)((id, attr, msg) => msg).cache()
      
      //count remaining undecided vertices
      val prev_count = remaining_vertices
      remaining_vertices = g.vertices.filter(_._2 == 0).count()
      

      val decided_this_round = prev_count - remaining_vertices
      val mis_vertices = g.vertices.filter(_._2 == 1).count()
      val excluded_vertices = g.vertices.filter(_._2 == -1).count()
      
      println(s"Iteration $iteration:")
      println(s"  MIS vertices: $mis_vertices")
      println(s"  Excluded vertices: $excluded_vertices")
      println(s"  Remaining active vertices: $remaining_vertices")
      println(s"  Vertices decided this round: $decided_this_round")
    }
    
    println(s"Luby's algorithm completed in $iteration iterations.")
    

    g
  }


  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    //check independence no two vertices in MIS should be connected
    val independenceCheck = g_in.aggregateMessages[Int](
      triplet => {
        if (triplet.srcAttr == 1 && triplet.dstAttr == 1) {
          triplet.sendToDst(1)
          triplet.sendToSrc(1)
        }
      },
      (a, b) => 1
    )
    
    if (independenceCheck.count() > 0) return false
    
    //no vertex outside MIS can be added without violating independence
    val maximalityCheck = g_in.aggregateMessages[Int](
      triplet => {
        //send 1 to non-MIS vertices if they have a neighbor in MIS
        if (triplet.srcAttr == 1 && triplet.dstAttr != 1) {
          triplet.sendToDst(1)
        }
        if (triplet.dstAttr == 1 && triplet.srcAttr != 1) {
          triplet.sendToSrc(1)
        }
      },
      (a, b) => 1
    )
    
    //get vertices not in MIS that have no neighbors in MIS
    val canAddVertices = g_in.vertices
      .leftJoin(maximalityCheck)((vid, attr, msg) => {
        if (attr != 1 && msg.isEmpty) 1 else 0
      })
      .filter(_._2 == 1)
      .count()
    
    canAddVertices == 0
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      if(ans)
        println("Yes")
      else
        println("No")
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }

  }
}
