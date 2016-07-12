package io.tengu.demo.summerschool2016;

import java.net.InetAddress;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

@SuppressWarnings("serial")
public class EsIndexBolt extends BaseBasicBolt {
	
	String clusterName;
	String[] nodes;
	
	TransportClient client = null;
	
	public EsIndexBolt(String clusterName, String[] nodes) {
		this.clusterName = clusterName;
		this.nodes = nodes;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		try {
			Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).build();
			TransportClient client = TransportClient.builder().settings(settings).build();
			for (String node : nodes) {
				String[] hp = node.split(":");
				client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hp[0]), Integer.parseInt(hp[1])));
			}
			this.client = client;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}



	@Override
	public void cleanup() {
		if(client != null) {
			client.close();
		}
	}



	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
            String source = input.getStringByField("source");
            String index = input.getStringByField("index");
            String type = input.getStringByField("type");
            String id = input.getStringByField("id");

            client.prepareIndex(index, type, id).setSource(source).execute().actionGet();
        } catch (Exception e) {
            collector.reportError(e);
        }
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
