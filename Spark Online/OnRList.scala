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

object OnRList {
 var g: TitanGraph = null 

 def main(args: Array[String]) {
	val sparkConf = new SparkConf().setAppName("OnRList").setMaster("local[4]")
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

	 		 		 
	 val v_cust= g.query().has("uid",10).vertices()

	 val c_recom_list = new scala.collection.mutable.Queue[Vertex]
    
	 
	 val n_vertex= v_cust.iterator.next().getVertices(Direction.OUT,"neigh").iterator()
	 while(n_vertex.hasNext()){
		   val cur_n_movis=n_vertex.next().getVertices(Direction.OUT,"relate").iterator()	
		   
		   while(cur_n_movis.hasNext()){
			val cur_n_movi=cur_n_movis.next()
			c_recom_list += cur_n_movi
			println("movi =" + cur_n_movi.getProperty("mname"))

		   }
	 }
	 
         g.shutdown()
	
 	 sc.stop()
 }
}
