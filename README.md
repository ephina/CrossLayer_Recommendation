# CrossLayer_Recommendation
Cross layer recommendation:

The distributed online-offline computation model consists of three layers: 
1.batch layer - Haddop Batch programs for data manupulation and learning
2.storage and compute layer - .groovy programs to batch upload the data from HDFS to form graph DB.
3.online layer - CMAB algorithm for online recommendation

The Hadoop batch layer uses the map-reduce procedure for the categorization of users based on multimedia diffusion in the society.  The derived user categorization along with the user and the multimedia content details are batch loaded to the storage and compute layer. The graph storage model is designed as a bi-partite property graph with users and multimedia content as vertices. The edge between the user and the multimedia content contains rating, date of rating and user adaptation category as its property. The graph based multi-armed bandit algorithm uses the solution of the user diffusion categorization problem for initializing the bandit problem. The randomized probability matching multi-armed bandit algorithm using fractional factorial probit predicts the probable neighbours of the user by considering the item affinity and the adaptation category. The Spark online layer reusing the working sets across iteration makes real time personalized recommendation. 
