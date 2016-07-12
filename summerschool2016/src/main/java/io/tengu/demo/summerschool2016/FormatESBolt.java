package io.tengu.demo.summerschool2016;

import java.util.ArrayList;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@SuppressWarnings("serial")
public class FormatESBolt extends BaseBasicBolt {
	
	String index;
	String type;
	
	public FormatESBolt(String index, String type) {
		this.index = index;
		this.type = type;
	}

	@SuppressWarnings("unchecked")
	public void execute(Tuple input, BasicOutputCollector collector) {
		ObjectMapper mapper = new ObjectMapper();
		
		ObjectNode source = mapper.createObjectNode();
		source.put("user", ""+input.getValueByField("user"));
		source.put("userId", (Long)input.getValueByField("userId"));
		source.put("text", ""+input.getValueByField("text"));

		Object tags = input.getValueByField("hashtags");
		if (tags != null && tags instanceof ArrayList) {
			ArrayNode hashtags = source.putArray("hashtags");
			for (String tag : (ArrayList<String>) tags) {
				hashtags.add(tag);
			}
		}
		
		tags = input.getValueByField("usertags");
		if (tags != null && tags instanceof ArrayList) {
			ArrayNode usertags = source.putArray("usertags");
			for (Long tag : (ArrayList<Long>) tags) {
				usertags.add(tag);
			}
		}
		
		System.out.println("TEST:TEST:TEST:::::"+source);
		
		collector.emit(new Values(""+input.getValueByField("id"), index, type, source.toString()));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "index", "type", "source"));
	}

}
