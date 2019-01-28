package stormProcessor;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.elasticsearch.bolt.EsIndexBolt;
import org.apache.storm.elasticsearch.bolt.EsPercolateBolt;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.elasticsearch.common.*;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

public class App 
{
	private static final String TOPOLOGY_NAME = "bitcoin-topology";
	private static final String CLUSTER_NAME = "bitcoin-cluster";
	private static final String[] CLUSTER_HOST =  {"192.168.0.24:9200"};
	
    public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException 
    {
    	Map<String,String> params = new HashMap<String,String>();
    	params.put("Content-Type", "application/json; charset=UTF-8");
    	EsConfig esConfig = new EsConfig("http://192.168.0.24:9200");
        EsTupleMapper tupleMapper = new MyEsTupleMapper();
    	TopologyBuilder builder = new TopologyBuilder();
    	
    	KafkaSpoutConfig.Builder<String, String> spoutConfigBuilderTransaction = KafkaSpoutConfig
    			.builder("localhost:19092,localhost:19093", "bitcoin-transaction"); 
    	spoutConfigBuilderTransaction.setGroupId("transactions-stats");
    	KafkaSpoutConfig.Builder<String, String> spoutConfigBuilderRate = KafkaSpoutConfig
    			.builder("localhost:19092,localhost:19093", "bitcoin-rate");
    	spoutConfigBuilderRate.setGroupId("rate-stats");
    	KafkaSpoutConfig.Builder<String, String> spoutConfigBuilderBlock = KafkaSpoutConfig
    			.builder("localhost:19092,localhost:19093", "bitcoin-block");
    	spoutConfigBuilderBlock.setGroupId("blocks-stats");

    	KafkaSpoutConfig<String, String> spoutConfigTransaction = spoutConfigBuilderTransaction.build();
    	KafkaSpoutConfig<String, String> spoutConfigRate = spoutConfigBuilderRate.build();
    	KafkaSpoutConfig<String, String> spoutConfigBlock = spoutConfigBuilderBlock.build();
    	builder.setSpout("transaction", new KafkaSpout<String, String>(spoutConfigTransaction));
    	builder.setSpout("rate", new KafkaSpout<String, String>(spoutConfigRate));
    	builder.setSpout("block", new KafkaSpout<String, String>(spoutConfigBlock));
    	builder.setBolt("transaction-parsing", new BoltTransactionParser(),2).shuffleGrouping("transaction");
    	builder.setBolt("rate-parsing", new BoltRateParser()).shuffleGrouping("rate");
    	builder.setBolt("block-parsing", new BoltBlockParser()).shuffleGrouping("block");
    	builder.setBolt("block-stats", new BoltBlockStats())
    						.allGrouping("rate-parsing")	
    						.shuffleGrouping("block-parsing");
    	builder.setBolt("transaction-stats", new BoltTransactionStats().withTumblingWindow(BaseWindowedBolt.Duration.minutes(2)),1)
    						.shuffleGrouping("rate-parsing")				
    						.shuffleGrouping("transaction-parsing");
    	builder.setBolt("Es-formater", new BoltEsFormatter(),2)
    						.shuffleGrouping("transaction-stats")
    						.shuffleGrouping("block-stats")
    						.shuffleGrouping("rate-parsing");	    	
    	
    	builder.setBolt("es-insert", new EsIndexBolt(esConfig, tupleMapper),2).shuffleGrouping("Es-formater");

    	StormTopology topology = builder.createTopology();

    	Config config = new Config();
    	/*timeout had to be greater than sliding windows duration*/
		config.setMessageTimeoutSecs(4000);
		//config.setDebug(true);
		config.setNumWorkers(2);
		config.setMaxTaskParallelism(12);

    	if(args.length > 0 && args[0].equals("remote")) {
    		StormSubmitter.submitTopology(TOPOLOGY_NAME, config, topology);
    	}
    	else {
    		LocalCluster cluster = new LocalCluster();
        	cluster.submitTopology(TOPOLOGY_NAME, config, topology);
    	}
    }
}
