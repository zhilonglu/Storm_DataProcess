package bolt;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;





//import org.apache.storm.guava.net.HostAndPort;//��ͬ�ӿ�
import redis.clients.jedis.HostAndPort;
import net.sf.json.JSONObject;
import redis.clients.jedis.JedisCluster;
import utils.ReadXML;
import utils.RedisPool;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * ��redis���н���
 * @author JYH
 *
 */
public class vehicleTrack extends BaseRichBolt{
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;  	
	
	//�źŵ�˯��ʱ��
	private static long sleepTime = ReadXML.SleepTime;
	private static int numOfEachSlot = ReadXML.NumOfEachSlot;
	private static int MaxSize = ReadXML.MaxSize;
	
	//private static int TTL = 1800;//����key�Ĺ���ʱ�䡣30����
	
	//DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	private DateFormat df = new SimpleDateFormat("yyyyMMddhhmmss");//����ʵ�����ڸ�ʽ��һ��ȷ��д��
	
	Set<HostAndPort> jedisClusterNodes=new HashSet<HostAndPort>();
	JedisCluster cluster;
	
	private static HashMap<String,String> map = new HashMap<String,String>();
	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {		
		this. collector = collector;	
		cluster = new JedisCluster(RedisPool.loadServers());
		map.put("SZ", "1");
		map.put("YD", "2");
		map.put("SQ", "3");
		map.put("AA", "4");
	}

 	@Override
	public void execute(Tuple tuple) {	
		String line = tuple.getString(0);
		JSONObject js = JSONObject.fromObject(line);

		if(line.contains("strCompanyId") && map.get(js.getString("strCompanyId").substring(4,6))!=null){
			String companyID = map.get(js.getString("strCompanyId").substring(4,6));
		
		//��֤ӯ�������ֶ���ȷ����˾�������0
		//if(line.contains("uiCompanyId") && !js.getString("uiCompanyId").equals("0")){	
		
			//ӯ��������ߣ�
			//String companyID = js.getString("uiCompanyId");
			
		//��֤ӯ�������ֶ���ȷ����˾�������0
//		if(line.contains("strCompanyId") && !js.getString("strCompanyId").equals("0")){	
//		
//			//ӯ��������ߣ�
//			String companyID = js.getString("strCompanyId");
			
			String vehNo = js.getString("strVin");
			String gpsTime = js.getString("strPositionTime");
			String lon02 = js.getString("lon02");
			String lat02 = js.getString("lat02");
			String gridNo3 = js.getString("gridNo");		
			String gridNo2 = gridNo3.substring(0,6);
			
			String direction = js.getString("uiDirection");
			String speed = js.getString("strSpeed");		

			
			//String companyID = js.getString("uiCompanyId");
			String status = js.getString("uiBizStatus");
			
//			companyID = "1";
//			status = "1";
			
			String manageNo3 = js.getString("manageNo");
			if(cluster.exists(vehNo)==false && !status.equals("4")){			
	//======�������������Ҳ�Ϊͣ��======================================================================
				//������ʻ��Ϣ����λ����������������������list
				
				cluster.lpush(vehNo, "0");//��ʼ�źŵ�ֱ����0�����������
				
				StringBuilder builder = new StringBuilder(gpsTime);
				builder.append(",").append(gridNo2).append(",").append(gridNo3)
						.append(",").append(manageNo3)
						.append(",").append(lon02).append(",").append(lat02).append(",").append(speed)
						.append(",").append(direction).append(",").append(companyID).append(",").append(status);	
				cluster.rpush(vehNo, builder.toString());
				//��������ʱ�䣬�Լ�ȥ�ж��Ƿ����			
//				try {
//					java.util.Date d = df.parse(gpsTime);
//					cluster.rpush(vehNo, String.valueOf(d.getTime()));//!!!!!!!!!!!!!!!!!!!!!!
//				} catch (ParseException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
				
				cluster.rpush(vehNo, String.valueOf(System.currentTimeMillis()));					
				
				//cluster.expire(vehNo, TTL);//���øó�����ʱ			
				
				String key1 = vehNo+"_slot";
				//�켣�����
				cluster.lpush(key1, gpsTime+","+lon02+","+lat02+","+speed+","+status);//��һ������ռ��һ��λ��	
				//���������������ֱ���복�ƺ�
				cluster.rpush(gridNo3, vehNo);
				cluster.rpush(gridNo2, vehNo);	
				
				String combineKey3 = manageNo3+"-"+companyID+"-"+status;
				if(!cluster.exists(combineKey3)){
					cluster.set(combineKey3, "1");
				}else{
					int newNum = Integer.parseInt(cluster.get(combineKey3))+1;
					cluster.set(combineKey3, String.valueOf(newNum));
				}
				
				cluster.lset(vehNo, 0, "1");//ȫ��������ɺ󣬰��źŵ���Ϊ1
				
				if(cluster.exists("allCar")==false){
					cluster.lpush("allCar", vehNo);
				}else{
					ArrayList<String> list = (ArrayList<String>) cluster.lrange("allCar", 0, -1);
					if(!list.contains(vehNo)){
						cluster.lpush("allCar", vehNo);
					}
					
				}
				
			}else if(cluster.exists(vehNo)==true && status.equals("4")){
				//���ƴ��ڣ���ͣ����=======================================================================================
				//ʱ��Ϊ����ʱ�䣬�����κβ���
				//ʱ����ӿ���һϵ��ɾ������������key������������key������������һ��list��ȥ���ó�
				String light = cluster.lindex(vehNo, 0);
				while(light.equals("0")){
					try {
						Thread.sleep(sleepTime);
					} catch (InterruptedException e) {					
						e.printStackTrace();
					}
					light = cluster.lindex(vehNo, 0);//��ȡһ�ν��бȽ�
				}
				cluster.lset(vehNo, 0, "0");//���ִ��Ȩ�����̰�״̬��Ϊno�������������̷߳���
				
				String output = cluster.lindex(vehNo, 1);
				String[] str = output.split(",");			
				String oldTime = str[0];
				
				//long oldTime = Long.parseLong(cluster.lindex(vehNo, 2));
				//dd��ʾgpsʱ��-��ǰ����ʱ�䣬>0��ʾgpsʱ����ӿ���
				long dd = -1;//Ĭ����Ҫ������
				try {
					java.util.Date d1 = df.parse(gpsTime);
					java.util.Date d2 = df.parse(oldTime);
					dd = d1.getTime()-d2.getTime();//���ʱ���ֵ
					dd = d1.getTime()-d2.getTime();
				} catch (ParseException e) {
					e.printStackTrace();
				}						
				if(dd>0){
					//����key��������������������keyɾ��

//					if(cluster.llen(gridNo3)==0){
//						cluster.lrem("gridNo3", 0, gridNo3);
//					}
					
					cluster.lrem("allCar", 0, vehNo);//ͣ���ˣ�����list�еĸó�ɾ��
					
					//String output = cluster.lindex(vehNo, 1);
					//String[] str = output.split(",");
					
					String oldManageNo3 = str[3];
					String oldCompanyID = str[8];
					String oldStatus = str[9];
					String oldKey = oldManageNo3+"-"+oldCompanyID+"-"+oldStatus;
					int oldNum = Integer.parseInt(cluster.get(oldKey));
					if(oldNum>=1 && cluster.exists(oldKey)){
						cluster.set(oldKey, String.valueOf(oldNum-1));
					}
					cluster.del(vehNo);
					cluster.lrem(gridNo2, 0, vehNo);
					cluster.lrem(gridNo3, 0, vehNo);
					
					
					
				}	
				cluster.lset(vehNo, 0, "1");//������ɿ��ԣ��źŵ�Ϊ1
				
			}else{
	//===========list�к������������Ҳ�Ϊͣ��======================================================================			
				//ʱ����ӿ��󣬸��³���key����Ϣ�������������Ʊ���������������
				//����ʱ���Ⱥ�˳�򣬶�����켣��
				String light = cluster.lindex(vehNo, 0);
				while(light.equals("0")){
					try {
						Thread.sleep(sleepTime);
					} catch (InterruptedException e) {					
						e.printStackTrace();
					}
					light = cluster.lindex(vehNo, 0);//��ȡһ�ν��бȽ�
				}
				cluster.lset(vehNo, 0, "0");//���ִ��Ȩ�����̰�״̬��Ϊno�������������̷߳���						
	//==============�����������Թ켣�����=================================================================
				String output = cluster.lindex(vehNo, 1);
				String[] str = output.split(",");
				
				String oldTime = str[0];
				//long oldTime = Long.parseLong(cluster.lindex(vehNo, 2));
				
				//dd��ʾgpsʱ��-��ǰ����ʱ�䣬>0��ʾgpsʱ����ӿ���
				long dd = -1;//Ĭ����Ҫ������
				try {
					java.util.Date d1 = df.parse(gpsTime);
					java.util.Date d2 = df.parse(oldTime);
					dd = d1.getTime()-d2.getTime();//���ʱ���ֵ
					dd = d1.getTime()-d2.getTime();
				} catch (ParseException e) {
					e.printStackTrace();
				}						
				if(dd>0){
	//==============������������gpsʱ����ӿ���=======================
					
				//**********************************���¶������񣬸�����������
					//String output = cluster.lindex(vehNo, 1);
					//String[] str = output.split(",");
					
					String oldGridNo2 = str[1];
					String oldGridNo3 = str[2];				
					boolean change_gridNo2 = false;
					change_gridNo2 = !oldGridNo2.equals(gridNo2);
					if(change_gridNo2){	
						cluster.lrem(oldGridNo2, 0, vehNo);
						cluster.lrem(oldGridNo3, 0, vehNo);
						cluster.rpush(gridNo2, vehNo);
						cluster.rpush(gridNo3, vehNo);
						
//						//�ھ�����������ɾ�����ƺ󣬿�����������Ƿ��г�����û�У���list��ɾ��
//						if(cluster.llen(oldGridNo3)==0){cluster.lrem("gridNo3", 0, oldGridNo3);}
						
						oldGridNo2 = gridNo2;//�Ѷ������������������£����Ѿɶ����;�����д��
						oldGridNo3 = gridNo3;
						
						
					}else{
						//�����������û�з����仯���ٽ�һ���Ƚ����������Ƿ����仯							
						boolean change_gridNo3 = false;//Ĭ�����񲻱仯
						change_gridNo3 = !oldGridNo3.equals(gridNo3);								
						if(change_gridNo3){		
							//�ɵ���������ɾ�����µ���������Ĳ���		
							//�����������
							cluster.lrem(oldGridNo3, 0, vehNo);		
							cluster.rpush(gridNo3, vehNo);
							
//							//�ھ�����������ɾ�����ƺ󣬿�����������Ƿ��г�����û�У���list��ɾ��
//							if(cluster.llen(oldGridNo3)==0){cluster.lrem("gridNo3", 0, oldGridNo3);}
							
							oldGridNo3 = gridNo3;//�Ѿɵ�����������£����Ѿ�����д��
						}
					}
					//**************************************************���¶������񣬸����������� done			

					String oldManageNo3 = str[3];
					String oldCompanyID = str[8];
					String oldStatus = str[9];			
			
					String oldKey = oldManageNo3+"-"+oldCompanyID+"-"+oldStatus;
					String newKey = manageNo3+"-"+companyID+"-"+status;
					if(!newKey.equals(oldKey)){
						//���   ����-��˾-״̬ �����仯��
						//�µ�key������ӣ��ɵ�key����ɾ�����ɵ�key�϶�����
						if(!cluster.exists(newKey)){
							cluster.set(newKey, "1");
						}else{
							int newNum = Integer.parseInt(cluster.get(newKey))+1;
							cluster.set(newKey, String.valueOf(newNum));
						}
						
						int oldNum = Integer.parseInt(cluster.get(oldKey));
						if(oldNum>=1 && cluster.exists(oldKey)){
							cluster.set(oldKey, String.valueOf(oldNum-1));
						}
					}
					
					StringBuilder builder = new StringBuilder(gpsTime);
					builder.append(",").append(oldGridNo2).append(",").append(oldGridNo3)
					.append(",").append(manageNo3)
					.append(",").append(lon02).append(",").append(lat02).append(",").append(speed)
					.append(",").append(direction).append(",").append(companyID).append(",").append(status);
					cluster.lset(vehNo, 1, builder.toString());	
					
//					//��������ʱ�䣬�Լ�ȥ�ж��Ƿ����			
//					try {
//						java.util.Date d = df.parse(gpsTime);
//						cluster.lset(vehNo, 2,String.valueOf(d.getTime()));//!!!!!!!!!!!!!!!!!!!!!!
//					} catch (ParseException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
					cluster.lset(vehNo, 2,String.valueOf(System.currentTimeMillis()));
					//cluster.expire(vehNo, TTL);//******************
				}				
				
				//ִ�й켣��������
				//==============����gpsʱ�䣬�Թ켣��������=======================				
				String key1 = vehNo+"_slot";
				long key1_size = cluster.llen(key1);
				if(key1_size<numOfEachSlot){
					cluster.lpush(key1, gpsTime+","+lon02+","+lat02+","+speed+","+status);
				}else{
					cluster.ltrim(key1, 0, (MaxSize-1));//һ��ɾ��500��
					cluster.lpush(key1, gpsTime+","+lon02+","+lat02+","+speed+","+status);
				}
				//ȫ��������ɺ��޸�Ϊyes�����������̷߳���
				cluster.lset(vehNo, 0, "1");
			}
			
			synchronized (collector){  
				collector.ack(tuple); 
			}
			synchronized (collector){ 
				collector.fail(tuple); 
			}	
		}
						
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
