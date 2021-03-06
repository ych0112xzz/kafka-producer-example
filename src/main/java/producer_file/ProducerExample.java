package producer_file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerExample {
	static long input_size;

	static class Produce implements Runnable {
		ProducerConfig config;
		Producer<String, String> producer;
		String input;
		String topic;
		String partitionKey;

		/**
		 * 
		 * @param config
		 *            producer端的配置
		 * @param input
		 *            输入文件
		 * @param topic
		 *            写入数据的topic
		 * @param partitionKey
		 *            存放消息的partition
		 */
		public Produce(ProducerConfig config, String input, String topic,
				String partitionKey) {
			this.input = input;
			this.topic = topic;
			this.config = config;
			this.partitionKey = partitionKey;

			// 第一个String表示Partition key的类型，第二个String表示message的类型
			producer = new Producer<String, String>(config);
		}

		public void run() {
			long start_time = System.currentTimeMillis();
			BufferedReader reader = null;
			try {
				File f = new File(input);
				reader = new BufferedReader(new InputStreamReader(
						new FileInputStream(input)));
				// 得到输入文件的大小，单位为字节
				input_size = f.length();
			} catch (Exception e) {
				System.err.println("输入文件错误");
				e.printStackTrace();
				System.exit(2);
			}

			while (true) {
				String line = null;
				try {
					line = reader.readLine();
					System.out.println(line);
				} catch (IOException e) {
					System.err.println("输入文件错误");
					e.printStackTrace();
					System.exit(2);
				}
				if (line == null) {
					break;
				}
				// 发送消息line到指定的topic下的partition，partitionKey、line都是String类型
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(
						topic, partitionKey, line);
				producer.send(data);
			}

			try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long end_time = System.currentTimeMillis();
			long timer = (end_time - start_time) / 1000;
			double in_size = input_size / (1024 * 1024);
			double speed = in_size / timer;

			System.out.println("start send file time："
					+ new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
							.format(new java.util.Date(start_time)));
			System.out.println("finish send file time："
					+ new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
							.format(new java.util.Date(end_time)));
			System.out.println("sent file size：(MB)" + in_size);
			System.out.println("sent file speed(MB/s)： " + speed);
			producer.close();
		}
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 7) {
			System.err
					.println("please input <inputFile> <topic> <sync> <ack> <batch> <partitionKey> <brokerIpList>");
			System.exit(2);
		}
		// 指定输入文件
		String input = args[0];
		// 指定写入数据的topic
		String topic = args[1];
		// 指定消息发送是同步还是异步
		String sync = args[2];
		// 0表示producer无须等待leader的确认；1表示leader replica确认收到数据；-1表示in-sync里的所有replica都确认收到数据。
		String ack = args[3];
		// async模式下的一批消息的数量
		String batch = args[4];
		// 指定存放消息的partition
		String partitionKey = args[5];
		// 将指定的brokerIplist转化成形如host1:port1,host2:port2的brokerlist
		String brokerList = args[6];
		//String[] broker = brokerIpList.split(",");
		/*String brokerList = "192.168.80." + broker[0] + ":9092";
		for (int i = 1; i < broker.length; i++) {
			brokerList += ",192.168.80." + broker[i] + ":9092";
		}*/

		Properties props = new Properties();

		// 可以只配置一个broker，不过建议最好至少配置2个broker，这样即使有一个broker宕机了，另一个也能及时接替工作
		props.put("metadata.broker.list", brokerList);
		// 指定了将message从Producer发送到Broker的序列化方式，参数key.serializer.class用于设置key序列化的方法，其默认值与serializer.class相同
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", ack);
		props.put("producer.type", sync);
		props.put("batch.num.messages", batch);

		ProducerConfig config = new ProducerConfig(props);

		Produce produce = new Produce(config, input, topic, partitionKey);
		produce.run();
	}
}
