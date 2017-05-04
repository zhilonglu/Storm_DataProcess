package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
public class vehicle_location_info_hdfs_byDay extends BaseRichBolt {
	public static final long serialVersionUID = 1L;
	public OutputCollector collector;
	public static FileSystem fs=null;
	public static Path p=null; 
	public static FSDataOutputStream outputStream = null;
	public static Configuration conf =null;
	public static InputStream in = null;
	public static OutputStream out = null;
	public static String currTime = "20161107";
	public static String hdfs_path = "hdfs://hdfs1.hhdata.com:8020/user/gps/VehGnss_new/";
	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		conf = new Configuration();
		conf.setBoolean("dfs.support.append", true);
		conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf .set("dfs.client.block.write.replace-datanode-on-failure.policy" ,"NEVER" );
		conf .set("dfs.client.block.write.replace-datanode-on-failure.enable" ,"true" );
	}
	@Override
	public void execute(Tuple tuple) {
		String line = tuple.getString(0);
		// TODO Auto-generated method stub	
		try{
			if(line.contains("strCompanyId")){//如果不包括strCompanyId，则丢弃这条数据
				JSONObject js = JSONObject.fromObject(line);
				String strPositionTime = js.getString("strPositionTime");//时间格式为20161010152039
				String companyID = js.getString("strCompanyId");//获取公司ID
				String companyName = companyID.substring(4,6);
				try {
					if(!currTime.equals(strPositionTime.substring(0,8)))//当前时间与currTime不相同
					{
						currTime = strPositionTime.substring(0,8);
					}
					fs = FileSystem.get(URI.create("hdfs://hdfs1.hhdata.com:8020/user/gps/VehGnss_new/"),conf);
					p = new Path(hdfs_path+currTime+"/"+companyName+".txt");
					if(!fs.exists(p))//如果文件不存在的话
					{
						outputStream = fs.create(p);
						outputStream.write((line+"\n").getBytes());
						outputStream.close();
					}
					else{//如果文件存在的话
						fs.setReplication(p, (short)3);
						in = new BufferedInputStream(new ByteArrayInputStream((line+"\n").getBytes()));
						out = fs.append(p); 
						IOUtils.copyBytes(in, out, conf);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}catch(Exception e){

		}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
	}
}
