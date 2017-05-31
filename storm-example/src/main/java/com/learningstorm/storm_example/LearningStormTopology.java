package com.learningstorm.storm_example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class LearningStormTopology {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		// Create an instance of TopologyBuilder class
		TopologyBuilder builder = new TopologyBuilder();
			
		// Set the Spout Class
		builder.setSpout("LearningStormSpout", new LearningStormSpout(), 2);

		// Set the Bolt Class
		builder.setBolt("LearningStormBolt", new LearningStormBolt(), 4).shuffleGrouping("LearningStormSpout");
		
		Config config = new Config();
		config.setDebug(true);
		
		// Create an instance of LocalCluster class for executing topology in local mode
		LocalCluster cluster = new LocalCluster();
		
		// LearningStormTopology is the name of submitted topology.
		cluster.submitTopology("LearningStormTopology", config, builder.createTopology());
		
		try
		{
			Thread.sleep(10000);
		}
		catch(Exception exception)
		{
			System.out.println("Thread interrupted exception : " + exception);
		}
		
		// Kill the LearningStormTopology
		cluster.killTopology("LearningStormTopology");
		
		// Shutdown the storm test cluster
		cluster.shutdown();
	}

}
