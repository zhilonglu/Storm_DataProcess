package bolt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import net.sf.json.JSONObject;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 过滤掉不需要的字段
 * 实现坐标84到02的转换
 * 添加02坐标的经纬度到json中
 * @author JYH
 *
 */

public class transGPS extends BaseRichBolt{
	
	private static final long serialVersionUID = -5653803832498574866L;
	
	private static double a = 6378245.0;
	private static double ee = 0.00669342162296594323;
	
	//public static BufferedWriter writer;
	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private OutputCollector collector;  
     public void prepare(Map config, TopologyContext context, OutputCollector collector) {  
    	 this. collector = collector;  
//    	 try {
//			writer = new BufferedWriter(new FileWriter(new File("/home/storm/storm.err")));
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
      }
	
	@Override
	public void execute(Tuple tuple) {
		
		//topo.countIn++;
		
		String line = tuple.getString(0);//json格式
		line.replace("\\", "");
		JSONObject js = JSONObject.fromObject(line);

//		double lon = Double.valueOf(jsOld.getString("lon"));
//		double lat = Double.valueOf(jsOld.getString("lat"));	
		String strLongitude = js.getString("strLongitude");
		String strLatitude = js.getString("strLatitude");
		
		boolean flag = true;//默认是没问题，传下去
		//根据真实数据的情况来添加对经纬度的限制条件
		if(strLongitude.length()>0 && strLatitude.length()>0 && !strLongitude.equals("null") && !strLatitude.equals("null")){
			double lon = Double.valueOf(strLongitude);
			double lat = Double.valueOf(strLatitude);			
			
			String lon02 = "";
			String lat02 = "";
			//超出中国，直接赋值为空，且不传下去
			if ((lon < 72.004 || lon > 137.8347)||(lat < 0.8293 || lat > 55.8271)){
				lon02 = "";
				lat02 = "";
				flag = false;
			}else{			
				double x = lon -105.0;
				double y = lat - 35.0;			
				double dLat = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * Math.sqrt(Math.abs(x));
				dLat += (20.0 * Math.sin(6.0 * x * Math.PI) + 20.0 * Math.sin(2.0 * x * Math.PI)) * 2.0 / 3.0;
				dLat += (20.0 * Math.sin(y * Math.PI) + 40.0 * Math.sin(y / 3.0 * Math.PI)) * 2.0 / 3.0;
				dLat += (160.0 * Math.sin(y / 12.0 * Math.PI) + 320 * Math.sin(y * Math.PI / 30.0)) * 2.0 / 3.0;		
	//================================================================			
				double xx = lon -105.0;
				double yy = lat - 35.0;
				double dLon = 300.0 + xx + 2.0 * yy + 0.1 * xx * xx + 0.1 * xx * yy + 0.1 * Math.sqrt(Math.abs(xx));
				dLon += (20.0 * Math.sin(6.0 * xx * Math.PI) + 20.0 * Math.sin(2.0 * xx * Math.PI)) * 2.0 / 3.0;
				dLon += (20.0 * Math.sin(xx * Math.PI) + 40.0 * Math.sin(xx / 3.0 * Math.PI)) * 2.0 / 3.0;
				dLon += (150.0 * Math.sin(xx / 12.0 * Math.PI) + 300.0 * Math.sin(xx / 30.0	* Math.PI)) * 2.0 / 3.0;
	//================================================================			
				double radLat = lat / 180.0 * Math.PI;
				double magic = Math.sin(radLat);
				magic = 1 - ee * magic * magic;
				double sqrtMagic = Math.sqrt(magic);
				dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * Math.PI);
				dLon = (dLon * 180.0) / (a / sqrtMagic * Math.cos(radLat) * Math.PI);
				double mgLat = lat + dLat;
				double mgLon = lon + dLon;
				
				lon02 = String.valueOf(mgLon);
				lat02 = String.valueOf(mgLat);
			}
	//=================================================================================================		
			
			if(flag){
				js.put("lon02", lon02);
				js.put("lat02", lat02);
				
				synchronized (collector){
					collector.emit(new Values(js.toString()));
				}	
				synchronized (collector){  
				    collector.ack(tuple);  
				}
				synchronized (collector){  
				    collector.fail(tuple);  
				}
			}
//			else{
//				try {
//					String time = String.valueOf(df.format(new Date()));
//					js.put("time_stamp", time);
//					js.put("process_name", "storm_err");
//					js.put("info", "wrong_GPS");
//					writer.write(js.toString()+"\n");
//					writer.flush();
//					//writer.close();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
		}	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("GPS02"));
	}

}
