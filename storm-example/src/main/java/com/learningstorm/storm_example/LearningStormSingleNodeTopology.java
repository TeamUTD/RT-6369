package com.learningstorm.storm_example;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class LearningStormSingleNodeTopology {

	public static void main(String[] args) {
		// Create an instance of TopologyBuilder class
		TopologyBuilder builder = new TopologyBuilder();
			
		// Set the Spout Class
		builder.setSpout("LearningStormSpout", new LearningStormSpout(), 4);

		// Set the Bolt Class
		builder.setBolt("LearningStormBolt", new LearningStormBolt(), 2).shuffleGrouping("LearningStormSpout");
		
		Config config = new Config();
		config.setNumWorkers(3);
		
		try
		{
			// This statement submit the topology on remote cluster. 
			// args[0] = name of topology
			StormSubmitter.submitTopology(args[0], config, builder.createTopology());
		}
		catch(AlreadyAliveException alreadyAliveException)
		{
			System.out.println(alreadyAliveException);
		}
		catch(InvalidTopologyException invalidTopologyException)
		{
			System.out.println(invalidTopologyException);
		}
	}
}
