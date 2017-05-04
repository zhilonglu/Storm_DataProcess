package bolt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import net.sf.json.JSONObject;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * 
 * @author guoxi
 *
 */
class UpdateRgstTask extends TimerTask{
	private Statement statement;
	String driver="com.mysql.jdbc.Driver";
	//String url="jdbc:mysql://sql:3306/test";
	String url="jdbc:mysql://DB1:3306/test";
	private Connection connector;

	@Override
	public void run() {
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}		
		try {
			//connector = DriverManager.getConnection(url, "hhdata", "123456");
			connector = DriverManager.getConnection(url);
			statement=connector.createStatement();
			String sql="select prov_city from rgst_city";
			ResultSet rs = statement.executeQuery(sql);

			synchronized(toDifferentManage.class){
				toDifferentManage.updateRgst(rs);
			}
			//rs用完之后再关connector
			connector.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	
}
public class toDifferentManage extends BaseRichBolt {
	private static final long serialVersionUID = 3556320450745625137L;
	private OutputCollector collector;  
	private Statement statement;
	private static List<String> rgstList=new ArrayList<String>();
	@Override
	public  void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector=collector;	
		Timer timer=new Timer();
		//启动定时更新任务，每500秒执行一次
		timer.schedule(new UpdateRgstTask(), 0, 500000);
		
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void updateRgst(ResultSet rs) throws SQLException{	
		rgstList.clear();
		while(rs.next()==true){
			rgstList.add(rs.getString("prov_city"));
		}
	}
	@Override
	public void execute(Tuple tuple) {
		JSONObject json = JSONObject.fromObject(tuple.getString(0));
		//获取省+市4位行政编号
		String prov_city = json.getString("manageNo").substring(0, 4);
		synchronized(toDifferentManage.class){
			//prov_city是变量
			if(rgstList.contains(prov_city))
			{
				synchronized (collector){ 
					//数据格式：key value topic   key暂时没用，编程需要
					collector.emit(new Values("gps",json.toString(),"emit"+prov_city));
				}	
				synchronized (collector){  
					collector.ack(tuple); 
				}
				synchronized (collector){ 
					collector.fail(tuple);
				}			
			}
		}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key","message","topic"));
	}	
}

