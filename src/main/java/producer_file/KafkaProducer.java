package producer_file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
    static long input_size;

    static class Produce implements Runnable {
        ProducerConfig config;
        Producer<String, String> producer;
        List<File> files;
        String topic;
        String partitionKey;
        int partitionNum = 1;
        Random random = new Random();
        String type = "0";
        int linePerSecond = Integer.MAX_VALUE;

        /**
         * @param config       producer端的配置
         * @param inputFile    输入文件
         * @param topic        写入数据的topic
         * @param partitionKey 存放消息的partition
         */
        public Produce(ProducerConfig config, List<File> inputFile, String topic,
                       int linePerSecond, String partitionKey, String type) {
            this.files = inputFile;
            this.topic = topic;
            this.config = config;
            this.linePerSecond = linePerSecond;
            this.partitionKey = partitionKey;
            this.type = type;

            // 第一个String表示Partition key的类型，第二个String表示message的类型
            producer = new Producer<String, String>(config);
        }

        public Produce(ProducerConfig config, List<File> inputFile, String topic,
                       int linePerSecond, int partitionNum, String type) {
            this.files = inputFile;
            this.topic = topic;
            this.config = config;
            this.linePerSecond = linePerSecond;
            this.partitionNum = partitionNum;
            this.type = type;

            // 第一个String表示Partition key的类型，第二个String表示message的类型
            producer = new Producer<String, String>(config);
        }


        public void run() {
            if (files != null) {
                int count = 0;
                long nextTime = System.currentTimeMillis() + 1000;
                for (int i = 0; i < files.size(); i++) {
                    long sentLines = 0;
                    long startTime = System.currentTimeMillis();
                    BufferedReader reader = null;
                    try {
                        reader = new BufferedReader(new InputStreamReader(
                                new FileInputStream(files.get(i))));
                        // get size(byte) of the input file
                        input_size = files.get(i).length();
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
                        } catch (IOException e) {
                            System.err.println("Inputfile error!");
                            e.printStackTrace();
                            System.exit(2);
                        }
                        count++;
                        sentLines++;
                        if (count == linePerSecond) {
                            long tmpTime = System.currentTimeMillis();
                            if (tmpTime < nextTime) {
                                try {
                                    Thread.sleep(nextTime - tmpTime);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                            nextTime += 1000;
                            count = 0;
                            System.out.println(this.toString() + ": sent lines: " + sentLines);
                        }
                        KeyedMessage<String, String> data;
                        if (type.equals("0")) {
                            data = new KeyedMessage<String, String>(
                                    topic, partitionKey, line);
                        } else {
                             data = new KeyedMessage<String, String>(
                                    topic, String.valueOf(random.nextInt(partitionNum)), line);
                        }
                        producer.send(data);
                    }
                    try {
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    long endTime = System.currentTimeMillis();
                    double timer = (double) (endTime - startTime) / 1000.0;
                    double in_size = input_size / (1024 * 1024);
                    double speedInMB = in_size / timer;
                    System.out.println(this.toString() + ": running time: " + timer + " s");
                    System.out.println(this.toString() + ": file name: "
                            + files.get(i).getName());
                    System.out.printf("%s: sent file size: %.2f MB\n",
                            this.toString(), in_size);
                    System.out.printf("%s: sent file speed: %.2f MB/s\n",
                            this.toString(), speedInMB);
                }
                producer.close();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err
                    .println("Usage: inputfile=/path/to/my/file topic=mytopic brokerlist=host1:port1,host2:port2 [sync=xxx] [ack=xxx] [partitionKey=xxx]|[partitionNum=xxx]");
            System.exit(2);
        }
        Map<String, String> inputs = new HashMap<String, String>();

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
                        .println("Usage: inputfile=/path/to/my/file topic=mytopic brokerlist=host1:port1,host2:port2 [sync=xxx] [ack=xxx] [partitionKey=xxx]|[partitionNum=xxx]");
                System.exit(2);
            }
            inputs.put(nameAndValue[0], nameAndValue[1]);
        }
        if ((!inputs.containsKey("inputfile"))
                || (!inputs.containsKey("topic"))
                || !inputs.containsKey("brokerlist")) {
            System.err.println("No inputfile or no topic or no brokerlist!");
            System.err
                    .println("Usage: inputfile=/path/to/my/file topic=mytopic brokerlist=host1:port1,host2:port2 [sync=xxx] [ack=xxx] [partitionKey=xxx]|[partitionNum=xxx]");
            System.exit(2);
        }
        //deal with inputfile
        File[] files = null;
        getInputFile(inputs, files);

        Integer threadNum = Integer.parseInt(inputs.get("threadNum"));

        Properties props = new Properties();
        props.put("metadata.broker.list", inputs.get("brokerlist"));
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", inputs.get("ack"));
        props.put("producer.type", inputs.get("sync"));
        props.put("batch.num.messages", inputs.get("batch"));

        ProducerConfig config = new ProducerConfig(props);


        ExecutorService exec = Executors.newFixedThreadPool(threadNum);
        List<List<File>> filesForEachThread = new ArrayList<List<File>>(
                threadNum);
        for (int i = 0; i < threadNum; i++) {
            filesForEachThread.add(new ArrayList<File>());
        }
        for (int i = 0; i < files.length; i++) {
            filesForEachThread.get(i % threadNum).add(files[i]);
        }
        int speed = 0;
        String speedInStr = null;
        if ((speedInStr = inputs.get("speed")) != null) {
            speed = Integer.parseInt(speedInStr);
        }
        for (int i = 0; i < threadNum; i++) {
            if (inputs.containsKey("partitionNum")) {
                if (inputs.containsKey("partitionKey")) {
                    Produce x = new Produce(config, filesForEachThread.get(i),
                            inputs.get("topic"), speed, inputs.get("partitionKey"), "0");
                    exec.execute(x);
                } else {
                    Produce x = new Produce(config, filesForEachThread.get(i),
                            inputs.get("topic"), speed, inputs.get("partitionKey"), "0");
                    exec.execute(x);
                }
            } else {
                Produce x = new Produce(config, filesForEachThread.get(i),
                        inputs.get("topic"), speed, Integer.parseInt(inputs.get("partitionNum")), "1");
                exec.execute(x);
            }

        }
        exec.shutdown();

    }

    private static void getInputFile(Map<String, String> inputs, File[] files) {
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
}
