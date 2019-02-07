package stormProcessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class BoltTransactionStats extends BaseWindowedBolt {
	private OutputCollector outputCollector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
	}
	@Override
	public void execute(TupleWindow inputWindow) {
		Integer tupleCount = 0;
		Double rate=0.;
		List<Tuple> anchors = new ArrayList<Tuple>();
		HashMap<String, HashMap<String, Object>> tuple_transactions= new HashMap<String, HashMap<String, Object>> ();
		Double amount=0.;
		Double transaction_total_amount=0.;
		Double transaction_total_amount_euro=0.;
		Double max_amount=0.;
		Double max_amount_euro=0.;
		String transaction_timestamp="";
		for(Tuple input : inputWindow.get()) {
			anchors.add(input);
			/*input bolt is transaction-parsing*/
			if(input.getSourceComponent().equals("transactionParsing")) {
				String transaction_hash = (String)input.getStringByField("transaction_hash");
				tuple_transactions.put(transaction_hash, new HashMap<String, Object>());
				tuple_transactions.get(transaction_hash).put("transaction_total_amount", input.getDoubleByField("transaction_total_amount"));
				//tuple_transactions.get(transaction_hash).put("transaction_timestamp", input.getDoubleByField("transaction_timestamp"));
				transaction_timestamp=(String)input.getStringByField("transaction_timestamp");
				tupleCount += 1;
				outputCollector.ack(input);
			}
			/*input bolt is rate-parsing*/
			else {
				rate = input.getDoubleByField("rate");
				outputCollector.ack(input);
			}
		}
		for(Entry<String, HashMap<String, Object>> transactions: tuple_transactions.entrySet()) {
			amount = (Double) transactions.getValue().get("transaction_total_amount");
			transaction_total_amount += amount;
			max_amount=amount>max_amount?amount:max_amount;
		}
		transaction_total_amount_euro = rate * transaction_total_amount;
		max_amount_euro = max_amount*rate;
		outputCollector.emit(anchors,new Values(max_amount,max_amount_euro,transaction_total_amount,transaction_total_amount_euro,tupleCount,transaction_timestamp));

	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("max_amount","max_amount_euro","transaction_total_amount","transaction_total_amount_euro","nb_transaction","transaction_timestamp"));
	}

}
