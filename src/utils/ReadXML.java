package utils;
/**
 * 配置读取
 * @author JYH
 */
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class ReadXML {
	private static String ConfigPath = "/home/storm/topo.xml";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
//topology的参数====================================================================
	public static String topic;
	
	//public static ArrayList<String> ZookeeperIPList = new ArrayList<String>();
	//public static int ZookeeperPort;
	//public static String ZKHosts;
	
	public static int NumWorkers;
	public static int NumKafkaSpout;
	public static int NumTransGPS;
	public static int NumAddGridNo;
	public static int NumAddManageNo;
	public static int NumVehicleTrack;	
	public static int NumAckers;
	public static int Mul;
//=========================================================================================	

	
	public static String CsvPath = "/home/storm/allGridToManage.csv";
	//若后期要指定睡眠时间和槽位最大数量，则将下面两个变量写入配置文件
	public static long SleepTime = 300;
	public static int NumOfEachSlot = 2000;
	public static int MaxSize = 1500;//满2000个后，删除500个，保留1500个轨迹点
//===========================================================================================
	//key是redis的ip地址，value是对应的端口号
	//public static HashMap<String,String> RedisHostAndPort = new HashMap<String,String>();
//=============================================================================================	
	public static void loadConfig() throws ParserConfigurationException, SAXException, IOException{
		File inputFile = new File(ConfigPath);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(inputFile);
		doc.getDocumentElement().normalize();
		
		//解析topic
		topic = String.valueOf(doc.getElementsByTagName("Topic").item(0).getTextContent());
//		//解析zookeeper的ip地址和端口
//		ZookeeperPort = Integer.parseInt(doc.getElementsByTagName("ZookeeperPort").item(0).getTextContent());
//		
//		String ipString = String.valueOf(doc.getElementsByTagName("ZookeeperIP").item(0).getTextContent());
//		String[] str1 = ipString.split(",");
//		int len1 = str1.length;
//		for(int i=0;i<len1;i++){
//			ZookeeperIPList.add(str1[i]);
//		}
//		//构造brokerHosts
//		for(int i=0;i<len1-1;i++){
//			ZKHosts += str1[i]+":"+ZookeeperPort+",";
//		}
//		ZKHosts += str1[len1-1]+":"+ZookeeperPort;
		
//		//解析redis的ip和端口
//		String redisInfo = String.valueOf(doc.getElementsByTagName("RedisHostAndPort").item(0).getTextContent());
//		String[] str2 = redisInfo.split(",");
//		int len2 = str2.length;
//		for(int i=0;i<len2;i++){
//			String[] str3 = str2[i].split(":");
//			RedisHostAndPort.put(str3[0], str3[1]);
//		}
		
		//解析一系列并发设置
		NumWorkers = Integer.parseInt(doc.getElementsByTagName("NumWorkers").item(0).getTextContent());
		NumKafkaSpout = Integer.parseInt(doc.getElementsByTagName("NumKafkaSpout").item(0).getTextContent());
		NumTransGPS = Integer.parseInt(doc.getElementsByTagName("NumTransGPS").item(0).getTextContent());
		NumAddGridNo = Integer.parseInt(doc.getElementsByTagName("NumAddGridNo").item(0).getTextContent());
		NumAddManageNo = Integer.parseInt(doc.getElementsByTagName("NumAddManageNo").item(0).getTextContent());
		NumVehicleTrack = Integer.parseInt(doc.getElementsByTagName("NumVehicleTrack").item(0).getTextContent());
		NumAckers = Integer.parseInt(doc.getElementsByTagName("NumAckers").item(0).getTextContent());
		Mul = Integer.parseInt(doc.getElementsByTagName("Mul").item(0).getTextContent());
		
		//CsvPath = String.valueOf(doc.getElementsByTagName("CsvPath").item(0).getTextContent());		
	}

}
