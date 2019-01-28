package stormProcessor;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;

public class BoltBlockParser extends BaseRichBolt {

private OutputCollector outputCollector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			process(input);
		} catch (ParseException e) {
			e.printStackTrace();
			outputCollector.fail(input);
		}
	}
	
	public void process(Tuple input) throws ParseException {
		JSONParser jsonParser = new JSONParser();
		JSONObject obj = (JSONObject)jsonParser.parse(input.getStringByField("value"));
		if(obj.containsKey("block_reward") && obj.containsKey("block_found_by") && obj.containsKey("block_timestamp")) {
			Double block_reward = (Double)obj.get("block_reward");
			String foundBy = (String)obj.get("block_found_by");
			String block_timestamp = (String)obj.get("block_timestamp");
			String block_hash = (String)obj.get("block_hash");
			outputCollector.emit(input,new Values(foundBy,block_timestamp,block_reward,block_hash));
			outputCollector.ack(input);
		}else {
			outputCollector.fail(input);
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("foundBy","block_timestamp","block_reward","block_hash"));
	}

}
