package io.tengu.demo.summerschool2016;

import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.topology.TopologyBuilder;

public class Demo2Topology {
	
	private static String topologyName = "Demo1";

	public static void main(String[] args) throws Exception {
		String remoteClusterTopologyName = null;
		//String broker = "localhost:9092";
		String broker = "143.129.91.110:9092";
		String esCluster = "elasticsearch";
		String[] esNodes = new String[]{"localhost:9300"};
		if (args.length >= 1) {
			remoteClusterTopologyName = args[0];
			if (args.length >= 2) {
				broker = args[1];
				if (args.length >= 3) {
					esCluster = args[2];
					if (args.length >= 4) {
						esNodes = new String[args.length-3];
						for (int i=0; i<esNodes.length; i++) {
							esNodes[i] = args[i+3];
						}
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

		Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaBolt kafkaBolt = new KafkaBolt().withProducerProperties(props)
				                             .withTopicSelector(new DefaultTopicSelector("demo2"))
				                             .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("id", "text"));
		builder.setBolt("kafka", kafkaBolt, 1).shuffleGrouping("derive");
		
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
