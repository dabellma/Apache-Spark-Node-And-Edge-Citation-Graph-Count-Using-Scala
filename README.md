For this project I correlated physics paper citations from multiple decades to generate a citation graph with millions of links. 
The idea was to find which paper citated which other papers, and then which papers cited those secondary papers, and so on, to create chains of citations,
creating links among citations from different decades.

I set up a Hadoop cluster and used Hadoop's distributed file system running with Spark to run my algorithm of getting neighboring nodes. This was done with the 
Resilient Distributed Dataset model to significantly speed up the calculations.
