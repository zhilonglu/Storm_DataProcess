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
import bolt.vehicle_location_info_hbase_special;
import bolt.vehicle_location_info_hdfs;
import bolt.vehicle_location_info_hdfs_byDay;
import bolt.writeToHDFS_test;
import bolt.writeToHbase_test;

/**
 * ���GPS���ݵ�����ţ��������ŵĹ�����ʵʱ����
 * GPS�����������ź�����ź���redis���н���
 * @author JYH
 *
 */
public class topo_test {	
	public static void main(String[] args) throws Exception {
		String zkRoot = "/kafka-storm";
		BrokerHosts brokerHosts = new ZkHosts("192.168.1.34:2181,192.168.1.35:2181,192.168.1.37:2181");
		//���ȼ�¼��ID
		String spoutId = "KafkaSpout";
		String Topic = "VEHGNSS";
		String TopoName = Topic+"2-Topology";
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,Topic , zkRoot, spoutId);
		spoutConfig.forceFromStart = true;//��Ϣ��ͷ��ʼ��ȡ--------------------------------------------------------
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		ArrayList<String> list = new ArrayList<String>();
		list.add("192.168.1.34");
		list.add("192.168.1.35");
		list.add("192.168.1.37");
		spoutConfig.zkServers = list;		
		spoutConfig.zkPort = 2181;
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("KafkaSpout",new KafkaSpout(spoutConfig),4);
//		builder.setBolt("transGPS",new transGPS(),20).setNumTasks(3*20).shuffleGrouping("KafkaSpout");
//		builder.setBolt("addGridNo",new addGridNo(),10).setNumTasks(3*10).shuffleGrouping("transGPS");
//		builder.setBolt("addManageNo",new addManageNo(),20).setNumTasks(3*20).shuffleGrouping("addGridNo");
//		builder.setBolt("vehicleTrack",new vehicleTrack(),20).setNumTasks(3*20).shuffleGrouping("addManageNo");
//		builder.setBolt("writeToHDFS", new writeToHDFS_test(),1).shuffleGrouping("KafkaSpout");
		builder.setBolt("writeToHbase", new writeToHbase_test(),1).shuffleGrouping("KafkaSpout");
		Config conf = new Config();
		conf.setNumWorkers(4);//������
		conf.setNumAckers(4);//acker��
		conf.setDebug(false);
		conf.setMessageTimeoutSecs(90);//�ύTopologyʱ�����ʵ�����Ϣ��ʱʱ�䣬Ĭ��30��
		//�ύ��stormִ��
		StormSubmitter.submitTopology(TopoName,conf, builder.createTopology());		
	}
}
