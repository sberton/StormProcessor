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

public class BoltRateParser extends BaseRichBolt {

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
		if (obj.containsKey("rate") && obj.containsKey("timestamp") ) {
			Double rate = (Double)obj.get("rate");
			String timestamp = (String)obj.get("timestamp");
			outputCollector.emit(input,new Values(rate,timestamp));
			outputCollector.ack(input);
		}else {
			outputCollector.fail(input);
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rate","timestamp"));
	}

}
