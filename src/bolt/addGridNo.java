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
 * ����02��γ�����꣬ת���ɿռ������
 * ��ӿռ�����ŵ�lineβ��
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
//		����02����ϵ�µľ�γ�ȼ���ռ�����ţ����뾭γ�ȣ����������=============================================		
		double longitude = Double.valueOf(js.getString("lon02"));
		double latitude = Double.valueOf(js.getString("lat02"));
		
		String gridNo = "";
		int lonCode, latCode, x1, y1, x2, y2;		
		//����1234λ
		double lonCode_Double = longitude - 60;
		double latCode_Double = latitude * 1.5;
		lonCode = (int) lonCode_Double;
		latCode = (int) latCode_Double;
		//����56λ
		double x1_Double = (lonCode_Double - lonCode) * 8;
		double y1_Double = (latCode_Double - latCode) * 8;
		x1 = (int) x1_Double;
		y1 = (int) y1_Double;	
		//����78λ
		x2 = (int) ((x1_Double - x1) * 10);
		y2 = (int) ((y1_Double - y1) * 10);		
		//ת����string	
		StringBuilder stringBuilder = new StringBuilder(String.valueOf(latCode));
		stringBuilder.append(String.valueOf(lonCode)).append(String.valueOf(y1)).append(String.valueOf(x1))
				.append(String.valueOf(y2)).append(String.valueOf(x2));
		gridNo = stringBuilder.toString();

		//��ӵ���ĩβ������
		js.put("gridNo", gridNo);
		
		//����һ��emit��ack��fail�ֿ�����
		synchronized (collector){ 
			collector.emit(new Values(js.toString()));
		}	
		synchronized (collector){  
			collector.ack(tuple); 
		}
		synchronized (collector){ 
			collector.fail(tuple);
		}
		
		
		//��������
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
		//������ź͵�GPS��¼
		declarer.declare(new Fields("GPSWithGridNo"));
	}

}
