import org.apache.hadoop.fs.Path
import com.tinkerpop.gremlin.scala.ScalaGraph
import com.thinkaurelius.titan.core.{TitanFactory, TitanGraph}
import com.tinkerpop.gremlin.scala.Gremlin
import org.apache.commons.configuration.BaseConfiguration

import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.Edge
import com.tinkerpop.blueprints.Direction

import scala.collection.JavaConverters._

import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark._

object HRecom {
 var g: TitanGraph = null 

 def getEdge(v:java.lang.Iterable[Vertex],d:Direction,l:String,p:String): scala.collection.mutable.Queue[String] ={
     val c_prop = new scala.collection.mutable.Queue[String]
     val edges= v.iterator.next().getEdges(d,l).iterator()
     while(edges.hasNext()){
		    c_prop += edges.next().getProperty(p)
     }
		 
     val l_prop =c_prop
     val c_prop_uni=l_prop.distinct
		 
     return c_prop_uni
   
 }
 def getNvertex(v:java.lang.Iterable[Vertex],d:Direction,l:String): scala.collection.mutable.Queue[Vertex] ={
     val c_Nvertexs = new scala.collection.mutable.Queue[Vertex]
    
	 
		 val n_vertex= v.iterator.next().getVertices(d,l).iterator()
		 while(n_vertex.hasNext()){
		   c_Nvertexs += n_vertex.next()
		   
		 }
		 val l_Nvertexs = c_Nvertexs
		 val c_Nvertexs_uni=l_Nvertexs.distinct
		 
		 return c_Nvertexs_uni
   
 }
 
 def main(args: Array[String]) {
	val sparkConf = new SparkConf().setAppName("HRecom").setMaster("local[4]")
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

	val aa = new scala.collection.mutable.Queue[String]
		
	 if(g == null || !g.isOpen()){
      			g = TitanFactory.open(conf);
         }
	 println(g)
	 
	 
	 val c_rate = new scala.collection.mutable.Queue[String]
	 val c_rdte = new scala.collection.mutable.Queue[String]
	 val c_ctgy = new scala.collection.mutable.Queue[String]
			
	 val c_ov = new scala.collection.mutable.Queue[Vertex]
	 val c_hop2_list = new scala.collection.mutable.Queue[Vertex]
		
		 		 
	 val v_cust= g.query().has("uid",10).vertices()
	 
         println("Getting edges")
	// val c_rate_uni=getEdge(v_cust,Direction.OUT,"relate","rate")
 	// val c_ctgy_uni=getEdge(v_cust,Direction.OUT,"relate","ctgy")
		 
 	 println("Getting 1-hop vertices (movies)")
	 val c_ov_uni=getNvertex(v_cust,Direction.OUT,"relate")
		 
	 println("outvertex"+c_ov_uni)
	 
	 var i=0
	 var j=0
 	 println("Getting 2-hop vertices (neighbour users)")
/*	 for(i <- 1 to c_ov_uni.length){
	   val temp_v=c_ov_uni.dequeue().getVertices(Direction.IN, "relate")
	   //g.query().has("mid",c_ov_uni.dequeue().getProperty("mid")).vertices()
	   val c_hop2 = getNvertex(temp_v,Direction.IN, "relate")
	   for(j <- 1 to c_hop2.length){
	     c_hop2_list += c_hop2.dequeue()
	     
	   }
	 
	 }*/


	 for(i <- 1 to c_ov_uni.length){
	   val n_movi = c_ov_uni.dequeue()
           println("for movie id"+n_movi.getProperty("mid"))
	   val temp_v=n_movi.getVertices(Direction.IN, "relate")
	   val temp_it=temp_v.iterator()
	   while(temp_it.hasNext()){
		   c_hop2_list += temp_it.next()
		   
           }
	   println("neighbour users = "+c_hop2_list.length)
	 }
	 
	 g.shutdown()
	
         sc.stop()
 }
}
