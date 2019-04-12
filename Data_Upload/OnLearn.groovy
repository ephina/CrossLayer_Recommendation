def g

no_of_neighs=20
no_of_iteration=5
g=TitanFactory.open("titan-project/conf/titan-hbase-es.properties")

users=g.query().has("label","user").has("uid",10).vertices().iterator().next()

Random ran=new Random()

class Global{
	static List<Vertex> neigh = new ArrayList<Vertex>();
	static Map<Vertex,List>  neigh_map = new HashMap<Vertex,List>();
	static Map<Vertex,Integer>  neigh_count = new HashMap<Vertex,Integer>();
	static Map<Vertex,Integer>  pred_neigh = new HashMap<Vertex,Integer>();
}

count=0
users.each{
	println "user= "+it.uid
	
	it.outE().each{
		println "processing..."
		cur_ctgy=it.ctgy
		cur_rate=it.rate
		cur_movi=it.inV().iterator().next()

		cur_mid=cur_movi.getProperty("mid")
		cur_mname=cur_movi.getProperty("mname")
		cur_mrev=cur_movi.getProperty("mrev")
		cur_mprod=cur_movi.getProperty("mprod")
		cur_mreldt=cur_movi.getProperty("mreldt")
		cur_mgen=cur_movi.getProperty("mgen")
		cur_mdur=cur_movi.getProperty("mdur")
		cur_mmpaa=cur_movi.getProperty("mmpaa")
		cur_mbudg=cur_movi.getProperty("mbudg")

		edges =it.inV().next().inE().iterator()
		edges.each{
			List<String> list = new ArrayList<String>();

			if(it.ctgy==(cur_ctgy-1) && it.rate==cur_rate){
				list.add(cur_mid)
				list.add(cur_mgen)
				list.add(cur_mmpaa)
				list.add(it.ctgy)
				list.add(cur_rate)

				neigh_ver = it.outV().iterator().next()
				if(neigh_ver!=users){
					Global.neigh_map.put(neigh_ver, list)
					found=Global.neigh_count.find{it.key==neigh_ver}
				
					if(found!=null){
						incre = found.value+1
						Global.neigh_count[neigh_ver]=incre
					} 
					else if(found==null){
						Global.neigh_count.put(neigh_ver,1)
					}
				}
			}
		}
		
	}

	Global.neigh_count = Global.neigh_count.sort {a, b -> b.value <=> a.value}

	println "neigh_count =" + Global.neigh_count.entrySet().toList()[0..<no_of_neighs]
	Global.neigh = Global.neigh_count.entrySet().key.toList()[0..<no_of_neighs]

	pa=1/no_of_neighs
	
	
	for (i = 0; i < no_of_iteration; i++) {
   		rand_index=(Math.abs(new Random().nextInt() % no_of_neighs))
		cur_rand_neigh=Global.neigh.get(rand_index)
		println "current random no =" + rand_index
		println "current random neighbour =" + cur_rand_neigh

		List<String> neigh_prop_map = Global.neigh_map.get(cur_rand_neigh)
		List<String> cur_neigh_movi_list = new ArrayList<String>();
		
		pba=1.0
		pab=1.0

		cur_rand_neigh.outE().each{
			if(it.ctgy==neigh_prop_map[3] && it.rate==neigh_prop_map[4] && it.inV().iterator().next().getProperty("mgen")==neigh_prop_map[1]){
					cur_neigh_movi_list.add(it.inV().iterator().next()) //predicted list
					pab=pab*(pba*pa) //training for prediction		
			}
		}
		
		if(Global.pred_neigh.get(cur_rand_neigh)==null){
			
			Global.pred_neigh.put(cur_rand_neigh,pab)
		}
	}
	
	println "predicted keys" + Global.pred_neigh.keySet()
	neighs=Global.pred_neigh.keySet() as List
	
	for (i = 0; i < Global.pred_neigh.size(); i++) {
		println "neighbours =" + neighs[i]
		cur_neigh_prob=Global.pred_neigh.get(neighs[i])
		println "neighbours prob=" + cur_neigh_prob
		
		Edge e = it.addEdge("neigh",neighs[i])
		e.setProperty("prob",cur_neigh_prob)
			
	}
}

g.shutdown()
