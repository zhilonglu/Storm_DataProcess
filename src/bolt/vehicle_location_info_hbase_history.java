package bolt;
//������λ��Ϣ����Hbase,��Ҫ������ʷ�켣��ѯ
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
public class vehicle_location_info_hbase_history extends BaseRichBolt {
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
		//		cfg.set("hbase.rootdir", "hdfs://redis1.hhdata.com:8020/apps/hbase/data");//ʹ��eclipseʱ�����������������޷���λ
		//		cfg.set("hbase.zookeeper.quorum", "redis1.hhdata.com,redis2.hhdata.com,sql1.hhdata.com");
		//		cfg.set("hbase.zookeeper.property.clientPort", "2181");
		//		cfg.set("zookeeper.znode.parent","/hbase-unsecure");

		cfg.set("hbase.rootdir", "hdfs://hdfs1.hhdata.com:8020/apps/hbase/data");//ʹ��eclipseʱ�����������������޷���λ
		cfg.set("hbase.zookeeper.quorum", "hdfs1.hhdata.com,hdfs2.hhdata.com,hdfs3.hhdata.com");
		cfg.set("hbase.zookeeper.property.clientPort", "2181");
		cfg.set("zookeeper.znode.parent","/hbase-unsecure");
		try {
			connection = ConnectionFactory.createConnection(cfg);
			//ʾ�����Ƕ�ͬһ��table���в��������ֱ�ӽ�Table����Ĵ���������prepare����boltִ�й����п���ֱ�����á�
			//			table = connection.getTable(TableName.valueOf("historyTrajectory"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	public void execute(Tuple tuple) {
		try {
			//				connection = ConnectionFactory.createConnection(cfg);
			//ʾ�����Ƕ�ͬһ��table���в��������ֱ�ӽ�Table����Ĵ���������prepare����boltִ�й����п���ֱ�����á�
			table = connection.getTable(TableName.valueOf("historyTrajectory_new"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//��tuple�л�ȡ����
		String line = tuple.getString(0);
		try{
			if(line.contains("{")&&line.contains("}")&&line.contains("strCompanyId")){//�Ϸ���json�ļ����н���
				if(rowNum%5000==0)
					putslist = new LinkedList<>();
				JSONObject js = JSONObject.fromObject(line);
				String strCompanyId = js.getString("strCompanyId");
				String strVin = js.getString("strVin");
				String strPositionTime = js.getString("strPositionTime");
				String strLongitude = js.getString("strLongitude");
				String strLatitude = js.getString("strLatitude");
				String strSpeed = js.getString("strSpeed");
				String manageNo = js.getString("manageNo");
				String uiDirection = js.getString("uiDirection");

				String uiDistId = js.getString("uiDistId");
				String uiVehCol = js.getString("uiVehCol");
				String uiCountPosition = js.getString("uiCountPosition");
				String uiCurCityId = js.getString("uiCurCityId");
				String strMileage = js.getString("strMileage");
				String strElevation = js.getString("strElevation");
				//String uiEncrypt = js.getString("uiEncrypt");
				String uiWarnStatus = js.getString("uiWarnStatus");
				String uiVehStatus = js.getString("uiVehStatus");
				String uiBizStatus = js.getString("uiBizStatus");				
				String lon02 = js.getString("lon02");
				String lat02 = js.getString("lat02");

				//����keyֵΪ���ƺ�+ʱ�䣬�м���#�ָ�������ʱ��ĸ�ʽ��20160916151820
				String sTime = strPositionTime.substring(0,4)+"-"+strPositionTime.substring(4,6)+"-"+
						strPositionTime.substring(6,8)+" "+strPositionTime.substring(8,10)+":"+
						strPositionTime.substring(10,12)+":"+strPositionTime.substring(12,14);
				String rkey = strVin+"#"+sTime;
				Put put = new Put(rkey.getBytes());
				put.addColumn("info".getBytes(), "strVin".getBytes(), strVin.getBytes());
				put.addColumn("info".getBytes(), "strPositionTime".getBytes(), strPositionTime.getBytes());
				put.addColumn("info".getBytes(), "strCompanyId".getBytes(), strCompanyId.getBytes());
				put.addColumn("info".getBytes(), "strLongitude".getBytes(), strLongitude.getBytes());
				put.addColumn("info".getBytes(), "strLatitude".getBytes(), strLatitude.getBytes());
				put.addColumn("info".getBytes(), "strSpeed".getBytes(), strSpeed.getBytes());
				put.addColumn("info".getBytes(), "manageNo".getBytes(), manageNo.getBytes());
				put.addColumn("info".getBytes(), "uiDirection".getBytes(), uiDirection.getBytes());

				put.addColumn("info".getBytes(), "uiDistId".getBytes(), uiDistId.getBytes());
				put.addColumn("info".getBytes(), "uiVehCol".getBytes(), uiVehCol.getBytes());
				put.addColumn("info".getBytes(), "uiCountPosition".getBytes(), uiCountPosition.getBytes());
				put.addColumn("info".getBytes(), "uiCurCityId".getBytes(), uiCurCityId.getBytes());
				put.addColumn("info".getBytes(), "strMileage".getBytes(), strMileage.getBytes());
				put.addColumn("info".getBytes(), "strElevation".getBytes(), strElevation.getBytes());
				//put.addColumn("info".getBytes(), "uiEncrypt".getBytes(), uiEncrypt.getBytes());
				put.addColumn("info".getBytes(), "uiWarnStatus".getBytes(), uiWarnStatus.getBytes());
				put.addColumn("info".getBytes(), "uiVehStatus".getBytes(), uiVehStatus.getBytes());
				put.addColumn("info".getBytes(), "uiBizStatus".getBytes(), uiBizStatus.getBytes());
				put.addColumn("info".getBytes(), "lon02".getBytes(), lon02.getBytes());
				put.addColumn("info".getBytes(), "lat02".getBytes(), lat02.getBytes());
				putslist.add(put);
				rowNum++;
				if(rowNum%5000==0)
					table.put(putslist);
			}
		}catch(Exception e){

		}

	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	}
}
