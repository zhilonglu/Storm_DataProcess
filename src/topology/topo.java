package topology;

import java.util.ArrayList;
import java.util.Properties;

import spout.MessageScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.trident.TridentKafkaState;
import utils.ReadXML;
import utils.TopicSelector;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import bolt.addGridNo;
import bolt.addManageNo;
import bolt.toDifferentManage;
import bolt.transGPS;
import bolt.vehicleTrack;
import bolt.vehicle_location_info_hbase_history;
import bolt.vehicle_location_info_hdfs_byDay;

/**
 * 完成GPS数据到网格号，到行政号的关联，实时处理
 * GPS数据添加网格号和区域号后，与redis进行交互
 * @author JYH
 *
 */
public class topo {	
	public static void main(String[] args) throws Exception {
//============读配置文件=========================================================================
		ReadXML.loadConfig();
		String Topic = ReadXML.topic;
		String TopoName = Topic+"-Topology";
		
		int NumWorkers = ReadXML.NumWorkers;
		int NumKafkaSpout = ReadXML.NumKafkaSpout;
		int NumTransGPS = ReadXML.NumTransGPS;
		int NumAddGridNo = ReadXML.NumTransGPS;
		int NumAddManageNo = ReadXML.NumAddManageNo;
		int NumVehicleTrack = ReadXML.NumVehicleTrack;
		int NumAckers = ReadXML.NumAckers; 
		int Mul = ReadXML.Mul;
//============storm从kafka读数据的配置=============================================================		
		//进度信息记录在哪个路径下
		String zkRoot = "/kafka-storm";
		//虽然叫brokerHosts，但是指定zookeeper的host		
		//世纪互联
		BrokerHosts brokerHosts = new ZkHosts("hdfs1.hhdata.com:2181,hdfs2.hhdata.com:2181,hdfs3.hhdata.com:2181");
		
		//盈都大厦
//		BrokerHosts brokerHosts = new ZkHosts("192.168.1.34:2181,192.168.1.35:2181,192.168.1.37:2181");
		//进度记录的ID
		String spoutId = "KafkaSpout";
		
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,Topic , zkRoot, spoutId);
		spoutConfig.forceFromStart = false;
		//spoutConfig.forceFromStart = true;//消息从头开始读取--------------------------------------------------------
		//spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());//自己定义的
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		ArrayList<String> list = new ArrayList<String>();
		
		//世纪互联
		list.add("192.168.1.101");
		list.add("192.168.1.102");
		list.add("192.168.1.103");
		
		//盈都
//		list.add("192.168.1.34");
//		list.add("192.168.1.35");
//		list.add("192.168.1.37");
		spoutConfig.zkServers = list;		
		spoutConfig.zkPort = 2181;
		//spoutConfig.bufferSizeBytes = 1024*1024;//SimpleConsumer所使用的SocketChannel的读缓冲区大小
		//spoutConfig.wait(1000);
//==============================================================================================	
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("KafkaSpout",new KafkaSpout(spoutConfig),NumKafkaSpout);			

		builder.setBolt("transGPS",new transGPS(),NumTransGPS).setNumTasks(Mul*NumTransGPS).shuffleGrouping("KafkaSpout");
		builder.setBolt("addGridNo",new addGridNo(),NumAddGridNo).setNumTasks(Mul*NumAddGridNo).shuffleGrouping("transGPS");
		builder.setBolt("addManageNo",new addManageNo(),NumAddManageNo).setNumTasks(Mul*NumAddManageNo).shuffleGrouping("addGridNo");
		builder.setBolt("vehicleTrack",new vehicleTrack(),NumVehicleTrack).setNumTasks(Mul*NumVehicleTrack).shuffleGrouping("addManageNo");	
		
		
		/**
		 * @author lzl
		 */
		builder.setBolt("vehicle_location_info_hdfs_byDay", new vehicle_location_info_hdfs_byDay(),1).shuffleGrouping("addManageNo");
		builder.setBolt("writeToHbase_history", new vehicle_location_info_hbase_history(),1).setNumTasks(1).shuffleGrouping("addManageNo");
		//builder.setBolt("writeToHbase_special", new vehicle_location_info_hbase_special(),1).shuffleGrouping("addManageNo");
		
		Config conf = new Config();
		conf.setNumWorkers(NumWorkers);//进程数
		conf.setNumAckers(NumAckers);//acker数
		conf.setDebug(false);
//**************************************************************************************************************************
		/**
		 * @author guoxi
		 */
		//=========storm写回kafka配置=============
//		KafkaBolt toKafka = new KafkaBolt();
//		toKafka.withTopicSelector(new TopicSelector())
//        	.withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
//		builder.setBolt("toDifferentManage", new toDifferentManage(),2).shuffleGrouping("addManageNo");
//		
//		builder.setBolt("toKafka",toKafka,2).shuffleGrouping("toDifferentManage");
//		//配置kafka producer
//        Properties props = new Properties();
//        props.put("metadata.broker.list", "kafka1:6667,kafka2:6667");
//        props.put("producer.type","async");
//        props.put("request.required.acks", "0"); // 0 ,-1 ,1
//        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);	
//**************************************************************************************************************************		
		//conf.setMaxTaskParallelism(MaxTaskParallelism);//每个Topology运行时最大的executor数目,线程数量
		//conf.setMaxSpoutPending(MaxSpoutPending);
		conf.setMessageTimeoutSecs(90);//提交Topology时设置适当的消息超时时间，默认30秒
		//conf.wait();//等到2000毫秒
		
		//conf.put(key, value);//对对应的配置项进行修改
		//conf.put("topology.error.throttle.interval.secs", 10);
		//conf.put("topology.disruptor.wait.strategy", "BlockingWaitStrategy");			
						
		//提交到storm执行
		StormSubmitter.submitTopology(TopoName,conf, builder.createTopology());		
	}
}
