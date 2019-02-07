package stormProcessor;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.shade.org.apache.commons.lang.math.RandomUtils;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import java.util.UUID;
//import static java.nio.charset.StandardCharsets.*;

public class BoltEsFormatter extends BaseRichBolt {

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
		JSONObject document = new JSONObject();
		HashMap<String,String> params = new HashMap<String,String>();
		
		String index="";
		String type="";
		String id=UUID.randomUUID().toString();
		if(input.getSourceComponent().equals("transactionStats")) {
			document.put("transaction_total_amount", input.getDoubleByField("transaction_total_amount"));
			document.put("transaction_total_amount_euro", input.getDoubleByField("transaction_total_amount_euro"));
			document.put("max_amount", input.getDoubleByField("max_amount"));
			document.put("max_amount_euro", input.getDoubleByField("max_amount_euro"));
			document.put("transaction_timestamp", input.getStringByField("transaction_timestamp"));
			document.put("nb_transaction", input.getIntegerByField("nb_transaction"));
			index="transactions";
			type= "transaction";
		} else if (input.getSourceComponent().equals("blockStats")) {
			document.put("foundBy", input.getStringByField("foundBy"));
			document.put("block_reward_euro", input.getDoubleByField("block_reward_euro"));
			document.put("block_timestamp", input.getStringByField("block_timestamp"));
			document.put("block_reward", input.getDoubleByField("block_reward"));
			index="blocks";
			type= "block";
		} else {
			document.put("rate", input.getDoubleByField("rate"));
			document.put("timestamp", input.getStringByField("timestamp"));
			index="rates";
			type= "rate";
		}		
		System.out.println(document);
		outputCollector.emit(input,new Values(document.toString(),index,type,id,params));
		outputCollector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("document","index","type","id","params"));
	}

}
