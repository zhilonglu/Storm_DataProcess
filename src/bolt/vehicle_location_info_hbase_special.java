package bolt;
//������λ��Ϣ����Hbase,��Ҫ�������⳵������
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.IOException;
import java.text.SimpleDateFormat;
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
public class vehicle_location_info_hbase_special extends BaseRichBolt {
	private OutputCollector collector;
	private static Connection connection = null;
	private static Table table = null;
	private static int rowNum;
	List<Put> putslist = null;
	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		rowNum = 0;
		Configuration cfg = HBaseConfiguration.create();
		cfg.set("hbase.rootdir", "hdfs://redis1.hhdata.com:8020/apps/hbase/data");//ʹ��eclipseʱ�����������������޷���λ
		cfg.set("hbase.zookeeper.quorum", "redis1.hhdata.com,redis2.hhdata.com,sql1.hhdata.com");
		cfg.set("hbase.zookeeper.property.clientPort", "2181");
		cfg.set("zookeeper.znode.parent","/hbase-unsecure");
		try {
			connection = ConnectionFactory.createConnection(cfg);
			//ʾ�����Ƕ�ͬһ��table���в��������ֱ�ӽ�Table����Ĵ���������prepare����boltִ�й����п���ֱ�����á�
			table = connection.getTable(TableName.valueOf("specialFound"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	public void execute(Tuple tuple) {
		try {
			//��tuple�л�ȡ����
			String line = tuple.getString(0);
			if(line.contains("{")&&line.contains("}")){//�Ϸ���json�ļ����н���
				if(rowNum%5000==0)
					putslist = new LinkedList<>();
				JSONObject js = JSONObject.fromObject(line);
				String uiCompanyId = js.getString("uiCompanyId");
				String gridNo = js.getString("gridNo");
				String strVin = js.getString("strVin");
				String strPositionTime = js.getString("strPositionTime");
				String strSpeed = js.getString("strSpeed");
				String manageNo = js.getString("manageNo");
				String uiDirection = js.getString("uiDirection");
				//����keyֵΪʱ��#��������ţ��м���#�ָ�������ʱ��ĸ�ʽ��20160916151820
				String sTime = strPositionTime.substring(0,4)+"-"+strPositionTime.substring(4,6)+"-"+
						strPositionTime.substring(6,8)+" "+strPositionTime.substring(8,10)+":"+
						strPositionTime.substring(10,12)+":"+strPositionTime.substring(12,14);
				String rkey = sTime+"#"+manageNo;
				Put put = new Put(rkey.getBytes());
				put.addColumn("info".getBytes(), "strVin".getBytes(), strVin.getBytes());
				put.addColumn("info".getBytes(), "gridNo".getBytes(), gridNo.getBytes());
				put.addColumn("info".getBytes(), "strPositionTime".getBytes(), strPositionTime.getBytes());
				put.addColumn("info".getBytes(), "uiCompanyId".getBytes(), uiCompanyId.getBytes());
				put.addColumn("info".getBytes(), "strSpeed".getBytes(), strSpeed.getBytes());
				put.addColumn("info".getBytes(), "manageNo".getBytes(), manageNo.getBytes());
				put.addColumn("info".getBytes(), "uiDirection".getBytes(), uiDirection.getBytes());
				putslist.add(put);
				rowNum++;
				if(rowNum%5000==0)
					table.put(putslist);
			}
		} catch (IOException e) {
			//do something to handle exception
		}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	}
}
