package bolt;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;





//import org.apache.storm.guava.net.HostAndPort;//不同接口
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
 * 与redis进行交互
 * @author JYH
 *
 */
public class vehicleTrack extends BaseRichBolt{
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;  	
	
	//信号灯睡眠时间
	private static long sleepTime = ReadXML.SleepTime;
	private static int numOfEachSlot = ReadXML.NumOfEachSlot;
	private static int MaxSize = ReadXML.MaxSize;
	
	//private static int TTL = 1800;//车牌key的过期时间。30分钟
	
	//DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	private DateFormat df = new SimpleDateFormat("yyyyMMddhhmmss");//根据实际日期格式进一步确定写法
	
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
		
		//保证盈都大厦字段正确，公司不会出现0
		//if(line.contains("uiCompanyId") && !js.getString("uiCompanyId").equals("0")){	
		
			//盈都大厦这边！
			//String companyID = js.getString("uiCompanyId");
			
		//保证盈都大厦字段正确，公司不会出现0
//		if(line.contains("strCompanyId") && !js.getString("strCompanyId").equals("0")){	
//		
//			//盈都大厦这边！
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
	//======不含这辆车，且不为停运======================================================================
				//车辆行驶信息，槽位，二三级网格，行政，车辆list
				
				cluster.lpush(vehNo, "0");//开始信号灯直接是0，不允许访问
				
				StringBuilder builder = new StringBuilder(gpsTime);
				builder.append(",").append(gridNo2).append(",").append(gridNo3)
						.append(",").append(manageNo3)
						.append(",").append(lon02).append(",").append(lat02).append(",").append(speed)
						.append(",").append(direction).append(",").append(companyID).append(",").append(status);	
				cluster.rpush(vehNo, builder.toString());
				//在最后加上时间，自己去判定是否过期			
//				try {
//					java.util.Date d = df.parse(gpsTime);
//					cluster.rpush(vehNo, String.valueOf(d.getTime()));//!!!!!!!!!!!!!!!!!!!!!!
//				} catch (ParseException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
				
				cluster.rpush(vehNo, String.valueOf(System.currentTimeMillis()));					
				
				//cluster.expire(vehNo, TTL);//设置该车过期时			
				
				String key1 = vehNo+"_slot";
				//轨迹表插入
				cluster.lpush(key1, gpsTime+","+lon02+","+lat02+","+speed+","+status);//这一行数据占用一个位置	
				//网格表，行政区划表分别插入车牌号
				cluster.rpush(gridNo3, vehNo);
				cluster.rpush(gridNo2, vehNo);	
				
				String combineKey3 = manageNo3+"-"+companyID+"-"+status;
				if(!cluster.exists(combineKey3)){
					cluster.set(combineKey3, "1");
				}else{
					int newNum = Integer.parseInt(cluster.get(combineKey3))+1;
					cluster.set(combineKey3, String.valueOf(newNum));
				}
				
				cluster.lset(vehNo, 0, "1");//全部操作完成后，把信号灯设为1
				
				if(cluster.exists("allCar")==false){
					cluster.lpush("allCar", vehNo);
				}else{
					ArrayList<String> list = (ArrayList<String>) cluster.lrange("allCar", 0, -1);
					if(!list.contains(vehNo)){
						cluster.lpush("allCar", vehNo);
					}
					
				}
				
			}else if(cluster.exists(vehNo)==true && status.equals("4")){
				//车牌存在，且停运了=======================================================================================
				//时间为乱序时间，不做任何操作
				//时间更加靠后，一系列删除操作：车牌key，二三级网格key，行政数量减一，list中去掉该车
				String light = cluster.lindex(vehNo, 0);
				while(light.equals("0")){
					try {
						Thread.sleep(sleepTime);
					} catch (InterruptedException e) {					
						e.printStackTrace();
					}
					light = cluster.lindex(vehNo, 0);//再取一次进行比较
				}
				cluster.lset(vehNo, 0, "0");//获得执行权，立刻把状态改为no，不允许其他线程访问
				
				String output = cluster.lindex(vehNo, 1);
				String[] str = output.split(",");			
				String oldTime = str[0];
				
				//long oldTime = Long.parseLong(cluster.lindex(vehNo, 2));
				//dd表示gps时间-当前最新时间，>0表示gps时间更加靠后
				long dd = -1;//默认是要丢掉的
				try {
					java.util.Date d1 = df.parse(gpsTime);
					java.util.Date d2 = df.parse(oldTime);
					dd = d1.getTime()-d2.getTime();//标记时间差值
					dd = d1.getTime()-d2.getTime();
				} catch (ParseException e) {
					e.printStackTrace();
				}						
				if(dd>0){
					//车牌key，二三级网格，行政区划key删除

//					if(cluster.llen(gridNo3)==0){
//						cluster.lrem("gridNo3", 0, gridNo3);
//					}
					
					cluster.lrem("allCar", 0, vehNo);//停运了，将车list中的该车删掉
					
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
				cluster.lset(vehNo, 0, "1");//操作完成可以，信号灯为1
				
			}else{
	//===========list中含有这辆车，且不为停运======================================================================			
				//时间更加靠后，更新车牌key中信息，二三级网格车牌变更，行政数量变更
				//不论时间先后顺序，都插入轨迹表
				String light = cluster.lindex(vehNo, 0);
				while(light.equals("0")){
					try {
						Thread.sleep(sleepTime);
					} catch (InterruptedException e) {					
						e.printStackTrace();
					}
					light = cluster.lindex(vehNo, 0);//再取一次进行比较
				}
				cluster.lset(vehNo, 0, "0");//获得执行权，立刻把状态改为no，不允许其他线程访问						
	//==============有这辆车，对轨迹表操作=================================================================
				String output = cluster.lindex(vehNo, 1);
				String[] str = output.split(",");
				
				String oldTime = str[0];
				//long oldTime = Long.parseLong(cluster.lindex(vehNo, 2));
				
				//dd表示gps时间-当前最新时间，>0表示gps时间更加靠后
				long dd = -1;//默认是要丢掉的
				try {
					java.util.Date d1 = df.parse(gpsTime);
					java.util.Date d2 = df.parse(oldTime);
					dd = d1.getTime()-d2.getTime();//标记时间差值
					dd = d1.getTime()-d2.getTime();
				} catch (ParseException e) {
					e.printStackTrace();
				}						
				if(dd>0){
	//==============有这辆车，且gps时间更加靠后=======================
					
				//**********************************更新二级网格，更新三级网格
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
						
//						//在旧三级网格中删除车牌后，看这个网格内是否还有车，若没有，从list中删除
//						if(cluster.llen(oldGridNo3)==0){cluster.lrem("gridNo3", 0, oldGridNo3);}
						
						oldGridNo2 = gridNo2;//把二级网格和三级网格更新，最后把旧二级和旧三级写入
						oldGridNo3 = gridNo3;
						
						
					}else{
						//如果二级网格没有发生变化，再进一步比较三级网格是否发生变化							
						boolean change_gridNo3 = false;//默认网格不变化
						change_gridNo3 = !oldGridNo3.equals(gridNo3);								
						if(change_gridNo3){		
							//旧的三级网格删除，新的三级网格的插入		
							//三级网格更新
							cluster.lrem(oldGridNo3, 0, vehNo);		
							cluster.rpush(gridNo3, vehNo);
							
//							//在旧三级网格中删除车牌后，看这个网格内是否还有车，若没有，从list中删除
//							if(cluster.llen(oldGridNo3)==0){cluster.lrem("gridNo3", 0, oldGridNo3);}
							
							oldGridNo3 = gridNo3;//把旧的三级网格更新，最后把旧三级写入
						}
					}
					//**************************************************更新二级网格，更新三级网格 done			

					String oldManageNo3 = str[3];
					String oldCompanyID = str[8];
					String oldStatus = str[9];			
			
					String oldKey = oldManageNo3+"-"+oldCompanyID+"-"+oldStatus;
					String newKey = manageNo3+"-"+companyID+"-"+status;
					if(!newKey.equals(oldKey)){
						//如果   行政-公司-状态 发生变化了
						//新的key进行添加，旧的key进行删除，旧的key肯定存在
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
					
//					//在最后加上时间，自己去判定是否过期			
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
				
				//执行轨迹表插入操作
				//==============不管gps时间，对轨迹表插入操作=======================				
				String key1 = vehNo+"_slot";
				long key1_size = cluster.llen(key1);
				if(key1_size<numOfEachSlot){
					cluster.lpush(key1, gpsTime+","+lon02+","+lat02+","+speed+","+status);
				}else{
					cluster.ltrim(key1, 0, (MaxSize-1));//一次删除500个
					cluster.lpush(key1, gpsTime+","+lon02+","+lat02+","+speed+","+status);
				}
				//全部处理完成后，修改为yes，允许其他线程访问
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
