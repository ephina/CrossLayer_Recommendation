import com.thinkaurelius.titan.core.Cardinality

def url
def g

g = TitanFactory.open("titan-project/conf/titan-hbase-es.properties")
g.shutdown()

com.thinkaurelius.titan.core.util.TitanCleanup.clear(g)

g = TitanFactory.open("titan-project/conf/titan-hbase-es.properties")
m = g.getManagementSystem()
user = m.makeVertexLabel("user").make()
movi = m.makeVertexLabel("movi").make()

m.makeEdgeLabel("relate").make()


uid = m.makePropertyKey("uid").dataType(Integer.class).make()

mid = m.makePropertyKey("mid").dataType(Integer.class).make()
mname = m.makePropertyKey("mname").dataType(String.class).make()
mrev = m.makePropertyKey("mrev").dataType(String.class).make()
mprod = m.makePropertyKey("mprod").dataType(String.class).make()
mreldt = m.makePropertyKey("mreldt").dataType(String.class).make()
mgen = m.makePropertyKey("mgen").dataType(String.class).make()
mdur = m.makePropertyKey("mdur").dataType(String.class).make()
mmpaa = m.makePropertyKey("mmpaa").dataType(String.class).make()
mbudg = m.makePropertyKey("mbudg").dataType(String.class).make()

rate = m.makePropertyKey("rate").dataType(String.class).make()
rdte = m.makePropertyKey("rdte").dataType(String.class).make()
ctgy = m.makePropertyKey("ctgy").dataType(String.class).make()

m.buildIndex("userById", Vertex.class).addKey(uid).indexOnly(user).buildCompositeIndex()
m.buildIndex("moviById", Vertex.class).addKey(mid).indexOnly(movi).buildCompositeIndex()

m.commit()
i=0

getOrCreateMovi={String... arr -> 
    if (arr[0].toInteger()<400){
    	def v = g.addVertexWithLabel("movi")
    	v.setProperty("mid", arr[0].toInteger())
    	v.setProperty("mname", arr[1])
    	v.setProperty("mrev", arr[2])
    	v.setProperty("mprod", arr[3])
    	v.setProperty("mreldt", arr[4])
    	v.setProperty("mgen", arr[5])
    	v.setProperty("mdur", arr[6])
    	v.setProperty("mmpaa", arr[7])
    	v.setProperty("mbudg", arr[8])
    }
}
checkMovi = { def mid ->
    def itty = g.query().has("label", "movi").has("mid", mid).vertices().iterator()
    if (itty.hasNext()) return itty.next()
    return null	
}
getOrCreateUser={String... arr -> 
	 def itty = g.query().has("label", "user").has("uid", arr[0].toInteger()).vertices().iterator()
    	 if (itty.hasNext()) return 
	 def v = g.addVertexWithLabel("user")
	 v.setProperty("uid", arr[0].toInteger())
	 arr[1].splitEachLine("\\|") { lines ->
		lines.each{ rating ->
			rating.splitEachLine(","){ values -> 
				vv= checkMovi(values[0])
				
				if(vv){
					println " its me ephina......"
					println vv
					Edge e = v.addEdge("relate",vv)
					e.setProperty("rate",values[1])
					e.setProperty("rdte",values[2])
					e.setProperty("ctgy",values[3])
				}
			}
		}
	 } 
}

//creating movie vertices with properties
url = "http://namenode:50070/webhdfs/v1/Movi/movie_desc?op=open".toURL().text
url.splitEachLine("\\|") { lines -> 		
	  if (lines.size()==9){
		getOrCreateMovi(*lines)
	  }
	if (++i % 10000 == 0) g.commit()
}

//creating user vertices with edges to the movie vertices created
url = "http://namenode:50070/webhdfs/v1/UsrPref/part-00000?op=open".toURL().text 
url.splitEachLine("\\s+") { lines -> 		
	getOrCreateUser(*lines)
	if (++i % 10000 == 0) g.commit()
}


g.shutdown()
/*
g=HadoopFactory.open("titan-project/conf/hadoop/input.properties")
g._()
*/

