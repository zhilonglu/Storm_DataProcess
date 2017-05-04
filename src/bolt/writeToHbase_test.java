package bolt;
//车辆定位信息存入Hbase,主要用于历史轨迹查询
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
public class writeToHbase_test extends BaseRichBolt {
	public OutputCollector collector;
	public static Connection connection = null;
	public static Table table = null;
	public static Configuration cfg=null;
	public static int rowNum;
	public static List<Put> putslist = null;
	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		rowNum = 0;
		cfg = HBaseConfiguration.create();
		cfg.set("hbase.rootdir", "hdfs://redis1.hhdata.com:8020/apps/hbase/data");//使用eclipse时必须添加这个，否则无法定位
		cfg.set("hbase.zookeeper.quorum", "redis1.hhdata.com,redis2.hhdata.com,sql1.hhdata.com");
		cfg.set("hbase.zookeeper.property.clientPort", "2181");
		cfg.set("zookeeper.znode.parent","/hbase-unsecure");
		try {
			connection = ConnectionFactory.createConnection(cfg);
			//示例都是对同一个table进行操作，因此直接将Table对象的创建放在了prepare，在bolt执行过程中可以直接重用。
//			table = connection.getTable(TableName.valueOf("historyTrajectory"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	public void execute(Tuple tuple) {
		try {
			try {
//				connection = ConnectionFactory.createConnection(cfg);
				//示例都是对同一个table进行操作，因此直接将Table对象的创建放在了prepare，在bolt执行过程中可以直接重用。
				table = connection.getTable(TableName.valueOf("test1019_1"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//从tuple中获取单词
			String line = tuple.getString(0);
				if(rowNum%5000==0)
					putslist = new LinkedList<>();
				JSONObject js = JSONObject.fromObject(line);
				String uiCompanyId = js.getString("uiCompanyId");
				String strVin = js.getString("strVin");
				String strPositionTime = js.getString("strPositionTime");
				String strLongitude = js.getString("strLongitude");
				String strLatitude = js.getString("strLatitude");
				String strSpeed = js.getString("strSpeed");
				String uiDirection = js.getString("uiDirection");
				String uiDistId = js.getString("uiDistId");
				String uiVehCol = js.getString("uiVehCol");
				String uiCountPosition = js.getString("uiCountPosition");
				String uiCurCityId = js.getString("uiCurCityId");
				String strMileage = js.getString("strMileage");
				String strElevation = js.getString("strElevation");
				String uiWarnStatus = js.getString("uiWarnStatus");
				String uiVehStatus = js.getString("uiVehStatus");
				String uiBizStatus = js.getString("uiBizStatus");				
				//设置key值为车牌号+时间，中间是#分隔，其中时间的格式是20160916151820
				String sTime = strPositionTime.substring(0,4)+"-"+strPositionTime.substring(4,6)+"-"+
						strPositionTime.substring(6,8)+" "+strPositionTime.substring(8,10)+":"+
						strPositionTime.substring(10,12)+":"+strPositionTime.substring(12,14);
				String rkey = strVin+"#"+sTime;
				Put put = new Put(rkey.getBytes());
				put.addColumn("info".getBytes(), "strVin".getBytes(), strVin.getBytes());
				put.addColumn("info".getBytes(), "strPositionTime".getBytes(), strPositionTime.getBytes());
				put.addColumn("info".getBytes(), "uiCompanyId".getBytes(), uiCompanyId.getBytes());
				put.addColumn("info".getBytes(), "strLongitude".getBytes(), strLongitude.getBytes());
				put.addColumn("info".getBytes(), "strLatitude".getBytes(), strLatitude.getBytes());
				put.addColumn("info".getBytes(), "strSpeed".getBytes(), strSpeed.getBytes());
				put.addColumn("info".getBytes(), "uiDirection".getBytes(), uiDirection.getBytes());
				put.addColumn("info".getBytes(), "uiDistId".getBytes(), uiDistId.getBytes());
				put.addColumn("info".getBytes(), "uiVehCol".getBytes(), uiVehCol.getBytes());
				put.addColumn("info".getBytes(), "uiCountPosition".getBytes(), uiCountPosition.getBytes());
				put.addColumn("info".getBytes(), "uiCurCityId".getBytes(), uiCurCityId.getBytes());
				put.addColumn("info".getBytes(), "strMileage".getBytes(), strMileage.getBytes());
				put.addColumn("info".getBytes(), "strElevation".getBytes(), strElevation.getBytes());
				put.addColumn("info".getBytes(), "uiWarnStatus".getBytes(), uiWarnStatus.getBytes());
				put.addColumn("info".getBytes(), "uiVehStatus".getBytes(), uiVehStatus.getBytes());
				put.addColumn("info".getBytes(), "uiBizStatus".getBytes(), uiBizStatus.getBytes());
				putslist.add(put);
				rowNum++;
				if(rowNum%5000==0)
					table.put(putslist);
		} catch (IOException e) {
			//do something to handle exception
		}

	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	}
}
