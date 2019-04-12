import org.apache.hadoop.fs.Path
import com.tinkerpop.gremlin.scala.ScalaGraph
import com.thinkaurelius.titan.core.{TitanFactory, TitanGraph}
import com.tinkerpop.gremlin.scala.Gremlin
import org.apache.commons.configuration.BaseConfiguration

import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.Edge
import com.tinkerpop.blueprints.Direction

import scala.collection.JavaConverters._
import scala.util.control.Breaks

import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark._

object Projr {
  var g: TitanGraph = null 
    
  def main(args: Array[String]) {
	val sparkConf = new SparkConf().setAppName("Projr").setMaster("local[4]")
 	val sc = new SparkContext(sparkConf)
		
 	val conf =  new BaseConfiguration()
 	val tableName = "titan"
    
	conf.setProperty("storage.backend", "hbase")
	conf.setProperty("storage.hostname", "192.168.109.171")
	conf.setProperty("storage.hbase-config.hbase.zookeeper.quorum", "192.168.109.171")
	conf.setProperty("storage.hbase-config.hbase.zookeeper.property.clientPort", "2181")
	conf.setProperty("storage.hostname", "192.168.109.171")
	conf.setProperty("storage.hbase.table", tableName)
		
	conf.setProperty("index.search.backend", "elasticsearch")
	conf.setProperty("index.search.hostname", "192.168.109.171")
	conf.setProperty("index.search.elasticsearch.cluster-name", "elasticsearch")
	conf.setProperty("index.search.elasticsearch.client-only", "true")

			
	if(g == null || !g.isOpen()){
      			g = TitanFactory.open(conf);
	}
	println(g)
	
	val outer = new Breaks;
 	val inner = new Breaks;
	val inner1 = new Breaks; 
	val c_hop2_vlist = new scala.collection.mutable.Queue[Vertex]
				 		 
	val v_cust = g.query().has("uid",10).vertices()
	val v_id  = v_cust.iterator.next()
 	val edges = v_id.getEdges(Direction.OUT,"relate").iterator()
 	println("reading edges of = "+ v_id)
 	outer.breakable {      
 		 while(edges.hasNext()){
        		val cur_edge = edges.next()
        		val cust_rate = cur_edge.getProperty("rate").asInstanceOf[String]
  	     		val cust_ctgy = cur_edge.getProperty("ctgy").asInstanceOf[String]
        		val cust_mov_wat = cur_edge.getVertex(Direction.IN)
	      		var cust_Nedge: Edge = null
		    
			println("movi watched = "+cust_mov_wat.getProperty("mid"))  
       			println("cust rate = "+cust_rate)	
        		println("cust ctgy = "+cust_ctgy) 
			val cust_neighs=cust_mov_wat.getVertices(Direction.IN, "relate").iterator()
		      	var cnt = 0
			      inner.breakable {  
				    while(cust_neighs.hasNext()){
	       
	      				val cur_neigh=cust_neighs.next()
	      	      			val cur_cust_edges= cur_neigh.getEdges(Direction.OUT, "relate").iterator()
	      	      			println("cust neighbour = "+ cur_neigh.getProperty("uid"))
	      	      			inner1.breakable {
  	      				while(cur_cust_edges.hasNext()){  
  	      					val cur_neigh_edge = cur_cust_edges.next()
						        if(cur_neigh == v_id){
  	      				    		inner1.break
  	      					}
  	      					if(cur_neigh_edge.getProperty("rate").asInstanceOf[String] == cust_rate){
  	      					  val cur_ctgy=cur_neigh_edge.getProperty("ctgy").asInstanceOf[String] 
  	      					  if((cur_ctgy.toInt) == ((cust_ctgy.toInt)-1) && !c_hop2_vlist.contains(cur_neigh)){
  	      					      c_hop2_vlist +=cur_neigh
  	      					      cnt += 1
  	      					      println("cust Neighbour count = "+ cnt)
  	      					  }
  	      					  
  	      					}
  	      				}  
	      				}//inner1 break	
				    }	
        	  }//inner break
		      println("total count="+cnt)
     		}
	}//outer break 
        g.shutdown()
        sc.stop()
   }
}



