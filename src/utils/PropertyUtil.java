package utils;

import java.io.InputStream;
import java.util.Properties;

import com.esotericsoftware.minlog.Log;

/**
 * �������ö�ȡ����
 * @author JYH
 */
public class PropertyUtil {

	private static Properties pros = new Properties();

	// ���������ļ�
	static {
		try {
			InputStream in = PropertyUtil.class.getClassLoader().getResourceAsStream("config.properties");
			pros.load(in);
		} catch (Exception e) {
			Log.error("load configuration error", e);
		}
	}

	/**
	 * ��ȡ�������е�����ֵ
	 * @param key
	 * @return
	 */
	public static String getProperty(String key) {
		return pros.getProperty(key);
	}

}
