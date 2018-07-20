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
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleProducer {

    static class Produce implements Runnable {
        double input_size;
        ProducerConfig config;
        Producer<String, String> producer;
        List<File> files;
        String topic;
        private Random random = new Random(47);
        private int linePerSecond = Integer.MAX_VALUE;
        public static int totalProduce = 0;
        private int id = totalProduce++;

        public Produce(ProducerConfig config, List<File> inputs, String topic,
                       int linePerSecond) {
            this.files = inputs;
            this.topic = topic;
            this.config = config;
            if (linePerSecond > 0) {
                this.linePerSecond = linePerSecond;
            }
            producer = new Producer<String, String>(config);
        }

        @Override
        public String toString() {
            return "Produce-" + id;
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
                        KeyedMessage<String, String> data = new KeyedMessage<String, String>(
                                topic, String.valueOf(random.nextInt()), line);
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
                    .println("Usage: inputfile=/path/to/my/file topic=mytopic brokerlist=host1:port1,host2:port2 [sync=xxx] [ack=xxx]");
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

        // check whether inputfile path is a directory
        File f = new File(inputs.get("inputfile"));
        File[] files = null;
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

        Integer threadNum = Integer.parseInt(inputs.get("threadNum"));

        Properties props = new Properties();
        props.put("metadata.broker.list", inputs.get("brokerlist"));
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", inputs.get("ack"));
        props.put("producer.type", inputs.get("sync"));
        props.put("batch.num.messages", inputs.get("batch"));

        ProducerConfig config = new ProducerConfig(props);

        if (files == null) {
            files = new File[1];
            files[0] = f;
        } else if (files.length == 0) {
            System.out
                    .println("The input directory is empty! No file will be sent.");
            System.exit(2);
        }

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
            Produce x = new Produce(config, filesForEachThread.get(i),
                    inputs.get("topic"), speed);
            exec.execute(x);
        }
        exec.shutdown();
    }
}

