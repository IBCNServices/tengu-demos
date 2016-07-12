package io.tengu.demo.summerschool2016;

import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class Demo1Topology {
	
	private static String topologyName = "Demo1";

	public static void main(String[] args) throws Exception {
		String remoteClusterTopologyName = null;
		String esCluster = "elasticsearch";
		String[] esNodes = new String[]{"localhost:9300"};
		if (args.length >= 1) {
			remoteClusterTopologyName = args[0];
			if (args.length >= 2) {
				esCluster = args[1];
				if (args.length >= 3) {
					esNodes = new String[args.length-2];
					for (int i=2; i<args.length; i++) {
						esNodes[i] = args[i];
					}
				}
			}
		}
		
		Properties twitterProps = new Properties();
		twitterProps.load(Demo2Topology.class.getClassLoader().getResource("twitter.properties").openStream());
		
		String consumerKey = twitterProps.getProperty("consumerKey");
	    String consumerSecret = twitterProps.getProperty("consumerSecret");
	    String accessToken = twitterProps.getProperty("accessToken");
	    String accessTokenSecret = twitterProps.getProperty("accessTokenSecret");
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("spout", new TwitterSampleSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, new String[]{}), 1);
		
		builder.setBolt("derive", new DeriveInformationBolt(), 1).shuffleGrouping("spout");
		
		builder.setBolt("format", new FormatESBolt("demo1", "tweet"), 1).shuffleGrouping("derive");
		
		EsIndexBolt indexBolt = new EsIndexBolt(esCluster, esNodes);
		builder.setBolt("elastic", indexBolt).shuffleGrouping("format");
        
		builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("format");
		
		
		Config conf = new Config();
        conf.setDebug(false);

    	if (remoteClusterTopologyName!=null) {
    		conf.setNumWorkers(3);
    		StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());

            Thread.sleep(10000);
            cluster.killTopology(topologyName);
            cluster.shutdown();
            System.exit(0);
        }
	}

}
