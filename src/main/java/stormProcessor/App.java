package stormProcessor;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

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
    	Properties appProps = new Properties();
        //String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String appConfigPath = "/home/serial/workspace/projet3/stormProcessor/conf/application.properties";
        try {
			appProps.load(new FileInputStream(appConfigPath));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
    	/*Spouts implementation*/
    	builder.setSpout("transaction", new KafkaSpout<String, String>(spoutConfigTransaction),Integer.parseInt(appProps.getProperty("transaction.paralellism")));
    	builder.setSpout("rate", new KafkaSpout<String, String>(spoutConfigRate),Integer.parseInt(appProps.getProperty("rate.paralellism")));
    	builder.setSpout("block", new KafkaSpout<String, String>(spoutConfigBlock),Integer.parseInt(appProps.getProperty("block.paralellism")));
    	
		/*Bolts implementation*/
    	builder.setBolt("transactionParsing", new BoltTransactionParser(),Integer.parseInt(appProps.getProperty("transactionParsing.paralellism")))
    						.shuffleGrouping("transaction");
    	builder.setBolt("rateParsing", new BoltRateParser(),Integer.parseInt(appProps.getProperty("rateParsing.paralellism")))
    						.shuffleGrouping("rate");
    	builder.setBolt("blockParsing", new BoltBlockParser(),Integer.parseInt(appProps.getProperty("blockParsing.paralellism")))
    						.shuffleGrouping("block");
    	builder.setBolt("blockStats", new BoltBlockStats(),Integer.parseInt(appProps.getProperty("blockStats.paralellism")))
    						.allGrouping("rateParsing")	
    						.shuffleGrouping("blockParsing");
    	builder.setBolt("transactionStats", new BoltTransactionStats().withTumblingWindow(BaseWindowedBolt.Duration.minutes(5)),Integer.parseInt(appProps.getProperty("transactionStats.paralellism")))
    						.shuffleGrouping("rateParsing")				
    						.shuffleGrouping("transactionParsing");
    	builder.setBolt("EsFormater", new BoltEsFormatter(),Integer.parseInt(appProps.getProperty("EsFormater.paralellism")))
    						.shuffleGrouping("transactionStats")
    						.shuffleGrouping("blockStats")
    						.shuffleGrouping("rateParsing");	    	
    	
    	builder.setBolt("esInsert", new EsIndexBolt(esConfig, tupleMapper),Integer.parseInt(appProps.getProperty("esInsert.paralellism")))
    						.shuffleGrouping("EsFormater");

    	StormTopology topology = builder.createTopology();

    	Config config = new Config();
    	/*timeout had to be greater than sliding windows duration*/
		config.setMessageTimeoutSecs(4000);
		//config.setDebug(true);
		config.setNumWorkers(Integer.parseInt(appProps.getProperty("num.workers")));
		//config.setMaxTaskParallelism(12);

    	if(args.length > 0 && args[0].equals("remote")) {
    		StormSubmitter.submitTopology(TOPOLOGY_NAME, config, topology);
    	}
    	else {
    		LocalCluster cluster = new LocalCluster();
        	cluster.submitTopology(TOPOLOGY_NAME, config, topology);
    	}
    }
}
