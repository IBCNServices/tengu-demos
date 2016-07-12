package io.tengu.demo.summerschool2016;

import java.util.ArrayList;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.UserMentionEntity;

@SuppressWarnings("serial")
public class DeriveInformationBolt extends BaseBasicBolt {

	public void execute(Tuple input, BasicOutputCollector collector) {
		Object obj = input.getValueByField("tweet");
		if (obj instanceof Status) {
			Status tweet = (Status) obj;
			
			if (!tweet.getText().contains("??")) {			
				ArrayList<String> hashtags = null;
				if (tweet.getHashtagEntities() != null) {
					hashtags = new ArrayList<String>();
					for (HashtagEntity tag: tweet.getHashtagEntities()) {
						hashtags.add(tag.getText());
					}
				}
				ArrayList<Long> usertags = null;
				if (tweet.getUserMentionEntities() != null) {
					usertags = new ArrayList<Long>();
					for (UserMentionEntity tag: tweet.getUserMentionEntities()) {
						usertags.add(tag.getId());
					}
				}
				collector.emit(new Values(""+tweet.getId(), tweet.getUser().getId(), tweet.getUser().getName(), tweet.getText(), hashtags, usertags));
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "userId", "user", "text", "hashtags", "usertags"));
	}

}
