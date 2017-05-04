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

import com.esotericsoftware.minlog.Log;
public class writeToHDFS_test extends BaseRichBolt {
	public static final long serialVersionUID = 1L;
	public OutputCollector collector;
	public static FileSystem fs=null;
	public static Path p=null; 
	public static FSDataOutputStream outputStream = null;
	public static Configuration conf =null;
	public static InputStream in = null;
	public static OutputStream out = null;
	public static String hdfs_path_old = "hdfs://redis1.hhdata.com:8020/user/lzl/";
	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		conf = new Configuration();
		conf.setBoolean("dfs.support.append", true);
	}
	@Override
	public void execute(Tuple tuple) {
		String line = tuple.getString(0);
		Log.error(line);
		// TODO Auto-generated method stub	
		try {
			fs = FileSystem.get(URI.create("hdfs://redis1.hhdata.com:8020/user/lzl/"),conf);
			p = new Path(hdfs_path_old+"test_1018_2.txt");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		synchronized (fs) {
			try {
				if(!fs.exists(p))//如果文件不存在的话
				{
					outputStream = fs.create(p);
					outputStream.write((line+"\n").getBytes());
					outputStream.close();
				}
				else{//如果文件存在的话
					fs = FileSystem.get(URI.create("hdfs://redis1.hhdata.com:8020/user/lzl/"),conf);
					fs.setReplication(p, (short)3);
					in = new BufferedInputStream(new ByteArrayInputStream((line+"\n").getBytes()));
					out = fs.append(p); 
					IOUtils.copyBytes(in, out, conf);
					//				out.close();
					//				IOUtils.closeStream(in);
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
