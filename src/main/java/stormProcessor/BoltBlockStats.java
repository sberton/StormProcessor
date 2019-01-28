package stormProcessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class BoltBlockStats extends BaseRichBolt {
	private OutputCollector outputCollector;
	private Double rate=0.;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
	}
	@Override
	public void execute(Tuple input) {
		
		//HashMap<String, HashMap<String, Object>> tuple_blocks = new HashMap<String, HashMap<String, Object>> ();
		if(input.getSourceComponent().equals("block-parsing")) {
				String block_hash=input.getStringByField("block_hash");
				Double block_reward = (Double) input.getDoubleByField("block_reward");
				Double block_reward_euro = rate * block_reward ;
				String foundBy = (String) input.getStringByField("foundBy");
				String block_timestamp = (String) input.getStringByField("block_timestamp");
				
				outputCollector.emit(input,new Values(foundBy,block_reward,block_reward_euro,block_timestamp));	
				outputCollector.ack(input);
		}
		/*input bolt is rate-parsing*/
		else {
			rate = input.getDoubleByField("rate");
			outputCollector.ack(input);
		}
	}
	

	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("foundBy","block_reward","block_reward_euro","block_timestamp"));
	}

}
