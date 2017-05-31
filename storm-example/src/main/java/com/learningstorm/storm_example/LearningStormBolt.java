package com.learningstorm.storm_example;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class LearningStormBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = 1L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		// Fetch the field "site" from input tuple
		String test = input.getStringByField("site");
		
		// Print the value of field "site" on console
		System.out.println("Name of input site is : " + test);

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
