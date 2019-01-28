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

public class BoltTransactionParser extends BaseRichBolt {

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
		if (obj.containsKey("transaction_total_amount")
				&& obj.containsKey("transaction_hash")
				&& obj.containsKey("transaction_timestamp")) {
			Double transaction_total_amount = (Double)obj.get("transaction_total_amount");
			String transaction_hash = (String)obj.get("transaction_hash");
			String transaction_timestamp = (String)obj.get("transaction_timestamp");
			
			outputCollector.emit(input,new Values(transaction_total_amount,transaction_hash,transaction_timestamp));
			outputCollector.ack(input);
			
		}else {
			outputCollector.fail(input);
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("transaction_total_amount","transaction_hash","transaction_timestamp"));
	}

}
