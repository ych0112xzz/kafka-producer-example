package producer_file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class MultiThreadProducer {

	private static final String FINISHED = "This MultiThreadProducer is finished!";

	private LinkedBlockingQueue<String> recordQueue = new LinkedBlockingQueue<String>(
			1000);

	private Map<String, String> inputs = new HashMap<String, String>();

	private File[] files;

	private ProducerConfig config;

	private int threadNum;

	public MultiThreadProducer(String[] args) {
		inputs.put("sync", "sync");
		inputs.put("ack", "-1");
		inputs.put("batch", "200");
		inputs.put("partitionKey", "0");
		inputs.put("threadNum", "1");
		for (int i = 0; i < args.length; i++) {
			String[] nameAndValue = args[i].split("=");
			if (nameAndValue.length != 2) {
				System.err.println("Wrong argument format: " + args[i]);
				System.err
						.println("Usage: inputfile=/path/to/my/file topic=mytopic brokerlist=host1:port1,host2:port2 [sync=xxx] [ack=xxx]");
				System.exit(2);
			}
			inputs.put(nameAndValue[0], nameAndValue[1]);
		}

		if ((!inputs.containsKey("inputfile"))
				|| (!inputs.containsKey("topic"))
				|| !inputs.containsKey("brokerlist")) {
			System.err.println("No inputfile or no topic or no brokerlist!");
			System.err
					.println("Usage: inputfile=/path/to/my/file topic=mytopic brokerlist=host1:port1,host2:port2 [sync=xxx] [ack=xxx]");
			System.exit(2);
		}

		getInputFileList();

		threadNum = Integer.parseInt(inputs.get("threadNum"));

		Properties props = new Properties();
		props.put("metadata.broker.list", inputs.get("brokerlist"));
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", inputs.get("ack"));
		props.put("producer.type", inputs.get("sync"));
		props.put("batch.num.messages", inputs.get("batch"));

		config = new ProducerConfig(props);
	}

	private void getInputFileList() {
		File f = new File(inputs.get("inputfile"));
		if (f.isDirectory()) {
			files = f.listFiles();
			Collections.sort(Arrays.asList(files), new Comparator<File>() {
				public int compare(File o1, File o2) {
					return o1.getName().compareTo(o2.getName());
				}
			});
		} else {
			if (!f.exists()) {
				System.err.println("Inputfile doesn't exist!");
				System.exit(2);
			}
		}
		if (files == null) {
			files = new File[1];
			files[0] = f;
		} else if (files.length == 0) {
			System.out
					.println("The input directory is empty! No file will be sent.");
			System.exit(2);
		}
	}

	public void start() {
		ExecutorService exec = Executors.newFixedThreadPool(threadNum);
		for (int i = 0; i < threadNum; i++) {
			Produce x = new Produce(config, inputs.get("topic"), recordQueue);
			exec.execute(x);
		}
		exec.shutdown();

		readFiles();
	}

	private void readFiles() {
		double inputSize = 0;
		for (int i = 0; i < files.length; i++) {
			long startTime = System.currentTimeMillis();
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new InputStreamReader(
						new FileInputStream(files[i])));
				// get size(byte) of the input file
				inputSize = files[i].length();
			} catch (Exception e) {
				System.err.println("Inputfile error!");
				e.printStackTrace();
				System.exit(2);
			}
			// System.out.println("producer_startTime"+System.currentTimeMillis());
			while (true) {
				String line = null;
				try {
					line = reader.readLine();
					if (line == null) {
						break;
					}
					recordQueue.put(line);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			long endTime = System.currentTimeMillis();
			double timer = (double) (endTime - startTime) / 1000.0;
			double inputSizeInMB = inputSize / (1024 * 1024);
			double speedInMB = inputSizeInMB / timer;
			System.out.println("运行时间(s)：" + timer);
			System.out.println("文件名：" + files[i].getName());
			System.out.printf("发送文件的总大小：(MB)%.2f\n",
					inputSizeInMB);
			System.out.printf("发送文件的总速率：(MB)%.2f\n",
					speedInMB);
		}
		try {
			for (int i = 0; i < threadNum; i++) {
				recordQueue.put(FINISHED);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 3) {
			System.err
					.println("Usage: inputfile=/path/to/my/file topic=mytopic brokerlist=host1:port1,host2:port2 [sync=xxx] [ack=xxx]");
			System.exit(2);
		}
		MultiThreadProducer p = new MultiThreadProducer(args);
		p.start();
	}

	private static class Produce implements Runnable {
		ProducerConfig config;
		Producer<String, String> producer;
		String topic;
		private Random random = new Random(47);
		private static int totalProduce = 0;
		private int id = totalProduce++;
		private LinkedBlockingQueue<String> recordQueue;

		public Produce(ProducerConfig config, String topic,
				LinkedBlockingQueue<String> recordQueue) {
			this.topic = topic;
			this.config = config;
			producer = new Producer<String, String>(config);
			this.recordQueue = recordQueue;
		}

		@Override
		public String toString() {
			return "Produce-" + id;
		}

		public void run() {
			while (true) {
				String line = null;
				try {
					line = recordQueue.take();
					if (line.equals(FINISHED)) {
						break;
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(
						topic, String.valueOf(random.nextInt()), line);
				producer.send(data);
			}
			producer.close();
		}
	}

}

