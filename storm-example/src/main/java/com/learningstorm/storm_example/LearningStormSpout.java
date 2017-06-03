package com.learningstorm.storm_example;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class LearningStormSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector mSpoutOutputCollector;
	private static final Map<Integer,String> map = new HashMap<Integer, String>();

	static
	{
		map.put(0, "google");
		map.put(1,  "facebook");
		map.put(2,  "twitter");
		map.put(3, "youtube");
		map.put(4, "linkedin");
	}
	
	public void nextTuple() {
		// Storm cluster repeatedly calls this method to emit a continuous stream of tuples
		final Random rand = new Random();
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("*********************")
		  .setOAuthConsumerSecret("******************************************")
		  .setOAuthAccessToken("**************************************************")
		  .setOAuthAccessTokenSecret("******************************************");
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
		
		// Generate the random number from 0 to 4.
		int randomNumber = rand.nextInt(5);
		this.mSpoutOutputCollector.emit(new Values(map.get(randomNumber)));
	}

	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) 
	{
		//Open the Spout
		mSpoutOutputCollector = spoutOutputCollector;
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Emit the tuple with field "site"
		declarer.declare(new Fields("site"));
	}

}
