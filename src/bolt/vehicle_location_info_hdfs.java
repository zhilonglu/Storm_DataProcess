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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
//最新版的写入HDFS文件，0916已经成功测试
class TimerTaskTest extends TimerTask{
	@Override
	public void run(){
		synchronized(vehicle_location_info_hdfs.class){
			vehicle_location_info_hdfs.closeFile();
		}
	}
}
public class vehicle_location_info_hdfs extends BaseRichBolt {
	public static final long serialVersionUID = 1L;
	public OutputCollector collector;
	public static FileSystem fs=null;
	public static Path p=null; 
	public static FSDataOutputStream outputStream = null;
	public static Configuration conf =null;
	public static InputStream in = null;
	public static OutputStream out = null;
	public static int numberCount=0;
//	public static String currTime = "20161012";
	public static String hdfs_path_old = "hdfs://hdfs1.hhdata.com:8020/user/lzl/test_0927";
	public static String hdfs_path = "";
//	public static String hdfs_path_new = "hdfs://redis1.hhdata.com:8020/user/lzl/new_data.txt";
	public static void closeFile(){
		numberCount++;
		try {
			fs.close();
			conf = new Configuration();
			try {
				fs = FileSystem.get(URI.create("hdfs://hdfs1.hhdata.com:8020/user/lzl/"),conf);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			hdfs_path = hdfs_path_old+"_"+numberCount+".txt";
			p = new Path(hdfs_path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		conf = new Configuration();
		try {
			fs = FileSystem.get(URI.create("hdfs://hdfs1.hhdata.com:8020/user/lzl/"),conf);
			p = new Path(hdfs_path_old+".txt");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Timer timer=new Timer();
		timer.schedule(new TimerTaskTest(),0,60000);
	}
	@Override
	public void execute(Tuple tuple) {
		String line = tuple.getString(0);
		synchronized(vehicle_location_info_hdfs.class){
			// TODO Auto-generated method stub		
			try {
				if(!fs.exists(p))//如果文件不存在
				{
					outputStream = fs.create(p);
					outputStream.write((line+"\n").getBytes());
					outputStream.close();
				}
				else{
					fs = FileSystem.get(URI.create("hdfs://hdfs1.hhdata.com:8020/user/lzl/"),conf);
					p = new Path(hdfs_path);
					fs.setReplication(p, (short)1);
					in = new BufferedInputStream(new ByteArrayInputStream((line+"\n").getBytes()));
					out = fs.append(p); 
					IOUtils.copyBytes(in, out, conf);
					out.close();
					IOUtils.closeStream(in);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
	}
}
