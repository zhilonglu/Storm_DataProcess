package utils;

import java.io.InputStream;
import java.util.Properties;

import com.esotericsoftware.minlog.Log;

/**
 * 属性配置读取工具
 * @author JYH
 */
public class PropertyUtil {

	private static Properties pros = new Properties();

	// 加载属性文件
	static {
		try {
			InputStream in = PropertyUtil.class.getClassLoader().getResourceAsStream("config.properties");
			pros.load(in);
		} catch (Exception e) {
			Log.error("load configuration error", e);
		}
	}

	/**
	 * 读取配置文中的属性值
	 * @param key
	 * @return
	 */
	public static String getProperty(String key) {
		return pros.getProperty(key);
	}

}
