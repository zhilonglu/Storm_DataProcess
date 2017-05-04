package bolt;

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
 * 根据02经纬度坐标，转换成空间网格号
 * 添加空间网格号到line尾部
 * @author JYH
 *
 */
public class addGridNo extends BaseRichBolt{
	private static final long serialVersionUID = -6586283337287975719L;

	private OutputCollector collector;  
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {   	
        this. collector = collector;  
      }
	
	@Override
	public void execute(Tuple tuple) {
		String line = tuple.getString(0);	
		JSONObject js = JSONObject.fromObject(line);
//		根据02坐标系下的经纬度计算空间网格号；输入经纬度，返回网格号=============================================		
		double longitude = Double.valueOf(js.getString("lon02"));
		double latitude = Double.valueOf(js.getString("lat02"));
		
		String gridNo = "";
		int lonCode, latCode, x1, y1, x2, y2;		
		//计算1234位
		double lonCode_Double = longitude - 60;
		double latCode_Double = latitude * 1.5;
		lonCode = (int) lonCode_Double;
		latCode = (int) latCode_Double;
		//计算56位
		double x1_Double = (lonCode_Double - lonCode) * 8;
		double y1_Double = (latCode_Double - latCode) * 8;
		x1 = (int) x1_Double;
		y1 = (int) y1_Double;	
		//计算78位
		x2 = (int) ((x1_Double - x1) * 10);
		y2 = (int) ((y1_Double - y1) * 10);		
		//转换成string	
		StringBuilder stringBuilder = new StringBuilder(String.valueOf(latCode));
		stringBuilder.append(String.valueOf(lonCode)).append(String.valueOf(y1)).append(String.valueOf(x1))
				.append(String.valueOf(y2)).append(String.valueOf(x2));
		gridNo = stringBuilder.toString();

		//添加到行末尾，发送
		js.put("gridNo", gridNo);
		
		//方案一：emit，ack，fail分开进行
		synchronized (collector){ 
			collector.emit(new Values(js.toString()));
		}	
		synchronized (collector){  
			collector.ack(tuple); 
		}
		synchronized (collector){ 
			collector.fail(tuple);
		}
		
		
		//方案二：
//		synchronized (collector){  
//			try{
//				collector.ack(tuple); 
//			}catch(Exception e){
//				//Log.error("ack error !");
//			} 
//		}
//		synchronized (collector){ 
//			try{
//			    collector.fail(tuple);
//			}catch(Exception e){
//				//Log.error("ack error !");
//			} 
//		} 
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		//带网格号和的GPS记录
		declarer.declare(new Fields("GPSWithGridNo"));
	}

}
