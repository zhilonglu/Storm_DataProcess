package utils;

/**
 * 自定义topic选择器，写回kafka所需。
 * @author guoxi
 *
 */
import backtype.storm.tuple.Tuple;
import storm.kafka.bolt.selector.KafkaTopicSelector;

public class TopicSelector implements KafkaTopicSelector{
	/**
	 * @author guoxi
	 */
	private static final long serialVersionUID = -5736819704395123128L;

	public String getTopic(Tuple tuple){
		return (String) tuple.getValueByField("topic");
	}

}
