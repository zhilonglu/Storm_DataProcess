/**
 * 类名称：RedisPool.java
 * @author JYH
 * 
 * 
 **/
package utils;

import java.util.HashSet;
import java.util.Set;

import redis.clients.jedis.HostAndPort;

public class RedisPool {
	
	public static Set<HostAndPort> loadServers() {
		Set<HostAndPort> servers = new HashSet<HostAndPort>();
		
//		servers.add(new HostAndPort("192.168.1.34",6379));
//		servers.add(new HostAndPort("192.168.1.34",6380));
//		servers.add(new HostAndPort("192.168.1.34",6381));
//		servers.add(new HostAndPort("192.168.1.34",6382));
//		servers.add(new HostAndPort("192.168.1.34",6383));
//		
//		servers.add(new HostAndPort("192.168.1.37",16379));
//		servers.add(new HostAndPort("192.168.1.37",16380));
//		servers.add(new HostAndPort("192.168.1.37",16381));
//		servers.add(new HostAndPort("192.168.1.37",16382));
//		servers.add(new HostAndPort("192.168.1.37",16383));

//		//世纪互联
		servers.add(new HostAndPort("192.168.1.81",6379));
		servers.add(new HostAndPort("192.168.1.81",6380));
		servers.add(new HostAndPort("192.168.1.81",6381));
		servers.add(new HostAndPort("192.168.1.81",6382));
		servers.add(new HostAndPort("192.168.1.81",6383));
		
		servers.add(new HostAndPort("192.168.1.82",16379));
		servers.add(new HostAndPort("192.168.1.82",16380));
		servers.add(new HostAndPort("192.168.1.82",16381));
		servers.add(new HostAndPort("192.168.1.82",16382));
		servers.add(new HostAndPort("192.168.1.82",16383));
		
//		//192.168.1.11:7000,192.168.1.11:7001,192.168.1.11:7002,192.168.1.15:17000,192.168.1.15:17001,192.168.1.15:17002
//		Iterator it = ReadXML.RedisHostAndPort.keySet().iterator();
//		while(it.hasNext()){
//			String redisIP = String.valueOf(it.next());
//			String redisPort = ReadXML.RedisHostAndPort.get(redisIP);
//			if(redisIP!=null && redisPort!=null){
//				servers.add(new HostAndPort(redisIP,Integer.parseInt(redisPort)));
//			}			
//		}
		return servers;
	}
    
}
