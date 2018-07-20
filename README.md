## Ŀ¼

- [kafka-producer-example�ļ�Ŀ¼�ṹ](http://gitlab.local/liuzheng/kafka/tree/master/kafka-producer-example#kafka-producer-example%E6%96%87%E4%BB%B6%E7%9B%AE%E5%BD%95%E7%BB%93%E6%9E%84)
- [��α���](http://gitlab.local/liuzheng/kafka/tree/master/kafka-producer-example#%E5%A6%82%E4%BD%95%E7%BC%96%E8%AF%91)
- [�������](http://gitlab.local/liuzheng/kafka/tree/master/kafka-producer-example#%E5%A6%82%E4%BD%95%E8%BF%90%E8%A1%8C)
- [ʵ����kafka��Ⱥ����](http://gitlab.local/liuzheng/kafka/tree/master/kafka-producer-example#%E5%AE%9E%E9%AA%8C%E5%AE%A4kafka%E9%9B%86%E7%BE%A4%E7%8E%AF%E5%A2%83)

### kafka-producer-example�ļ�Ŀ¼�ṹ

src/main/java�°����������̡�

- һ����producer_file,��ȡָ���ı����ļ������kafka topic������Ϣ��
- һ����producer_monitor,��ȡָ���ı����ļ����е��ļ������kafka topic������Ϣ����ظ��ļ��С�

### ��α���

> ����Ѿ���װ��maven��ʹ��git clone����repo������֮����Eclipse����ֱ�Ӵ�File -\> import -\> Maven -\> Existing Maven Projects������Ŀ���������ɺ�������1��3����

1. ��Eclipse�д����µ�Maven��[Maven�Ľ���](http://m.oschina.net/blog/145869)��[Maven��eclipse�еİ�װ](https://www.ibm.com/developerworks/cn/java/j-lo-maven/)����Ŀ����src/main/javaĿ¼�������ļ�������Ŀ��src�С�
2. ��д�½��õ�maven��Ŀ���pom.xml�ļ����ɲ��������[pom.xml](http://gitlab.local/liuzheng/kafka/blob/master/kafka-producer-example/pom.xml)�ļ���
3. eclipse���һ���ĿRun As -\> Maven build -\> Goals��clean package,������targetĿ¼��������Ӧ��jar��

**ע�⣺** ���Eclipse�е�Window�������˵������Preference���ڵ����Ĳ˵������ҵ�Java������������˵��е�Installed JREs��ѡ�����JDK��·����JDK������JRE�����һ��ʼ��·����JRE��ĳ�JDK������Ӧ����H:\Program Files\Java\jdk1.7.0_51\jre������H:\Program Files\Java\jre7����ѡ��Edit���ڵ����Ĳ˵�����ָ��Default VM arguments��Ϊ-Dmaven.multiModuleProjectDirectory=$M2_HOME

### �������

- ����target�µ�kafka-producer-example-0.0.1-SNAPSHOT.jar
  1. jar���ŵ���Ⱥ�϶�Ӧ�Լ���Ŀ¼��
  2. ʹ�������������У�
     - ��ȡָ���ı����ļ������kafka topic������Ϣ

         ```
         java -cp jarname packagename.Classname inputfile topic sync/async -1/0/1 anyInteger partition brokerIdList
         ```

     - ʾ����

         ```
         java -cp kafka-producer-example-0.0.1-SNAPSHOT.jar producer_file.ProducerExample /home/liuzheng/input topicLZ async 1 600 0 71,72
         ```
     
     - ��ȡָ���ı����ļ����е��ļ������kafka topic������Ϣ����ظ��ļ���

         ```
         java -cp jarname packagename.Classname inputfile topic sync/async -1/0/1 anyInteger partition brokerIdList
         ```

     - ʾ����

         ```
         java -cp kafka-producer-example-0.0.1-SNAPSHOT.jar producer_monitor.ProducerExample /home/liuzheng/inputdir topicLZ async 1 600 0 71,72
         ```
     
### ʵ����kafka��Ⱥ����

���е�Kafka��Ⱥ���£�
1. ���Լ�Ⱥ��BrokerΪ��80.71-80.75:9092�����ӵ�zookeeperΪ:datanode11-15:2181����Ҫ����ƽʱ������ϰ�����Դ�����ͨ�û���topic��[kafka consumer offset monitor](http://192.168.80.71:8080/#/)
2. ������Ⱥ��BrokerΪ��80.76-80.80:9092�����ӵ�zookeeperΪ:datanode21-25:2181����Ҫ����ʵ���һ����ճ��ϴ��������Դ�����ͨ�û���topic,[kafka consumer offset monitor](http://192.168.80.76:8080/#/)

Ҫʹ��Kafka���Լ�Ⱥ����������ϵ�������ڼ�Ⱥ�������Լ���topic