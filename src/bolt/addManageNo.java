package bolt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import utils.ReadXML;
import net.sf.json.JSONObject;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * ���ݿռ�����ţ�����������ţ���ӵ�lineβ��
 * @author JYH
 *
 */
public class addManageNo extends BaseRichBolt{
	private static final long serialVersionUID = 1L;
	
	private OutputCollector collector;  
	
	private static String path = ReadXML.CsvPath;	
	private BufferedReader reader = null;
	private static HashMap<String,String> gridTOmanage=new HashMap<String,String>();

	@Override
	public  void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this. collector = collector; 
		//String path = ReadXML.CsvPath;
		try {
			//������ź����������Ŷ�Ӧ���ļ�������HashMap
			reader = new BufferedReader(new FileReader(path));
			String line = "";
			String[] strs;
			String gridNo,manageNo;
			while((line=reader.readLine())!=null){
				strs = line.split(",");
				gridNo=strs[0];
				manageNo=strs[1];
				gridTOmanage.put(gridNo, manageNo);
			}			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	@Override
	public void execute(Tuple tuple) {
		String line = tuple.getString(0);
		
//============================================================================================
		JSONObject js = JSONObject.fromObject(line);
		String gridNo = js.getString("gridNo");
		String manageNo = "";
		if(gridTOmanage.get(gridNo)!=null){
			manageNo = gridTOmanage.get(gridNo);
			js.put("manageNo", manageNo);
		}else{
			js.put("manageNo", "");
		}
		
//		String manageNo = "";
//		if(gridNo.equals("")){
//			js.put("manageNo", "");
//		}else{
//			if(gridTOmanage.get(gridNo)!=null){
//				manageNo = gridTOmanage.get(gridNo);
//				js.put("manageNo", manageNo);
//			}else{
//				js.put("manageNo", "");
//			}
//		}
		
		
//==============================================================================================		

		
		//����һ��emit��ack��fail�ֿ�����
		if(manageNo.length()==6){
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
 		 
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		//������ź͵�GPS��¼
		declarer.declare(new Fields("GPSWithManageNo"));
	}

}
