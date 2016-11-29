package com.yonyou.myTest;

import java.util.*;

import com.yonyou.conf.ConfigurationManager;
import com.yonyou.constant.Constants;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

import com.google.common.base.Optional;

import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

public class UpdateStateByKeyWordCount {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
                .setAppName("logAnalysisStatSpark")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.streaming.blockInterval", "100")//ms→RDD
                .set("spark.streaming.unpersist", "true")
                .set("spark.shuffle.io.maxRetries", "60")
                .set("spark.shuffle.io.retryWait", "60s")
                .set("spark.reducer.maxSizeInFlight", "12")
                .set("spark.streaming.receiver.writeAheadLog.enable", "true");
        conf.setMaster("local[2]");//本地测试
        JavaStreamingContext jssc = new JavaStreamingContext(
                conf, Durations.seconds(5));
        //设置spark容错点
        jssc.checkpoint(ConfigurationManager.getProperty(Constants.STREAMING_CHECKPOINT));
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list",
                ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
        // 构建topic set
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(",");
        Set<String> topics = new HashSet<String>();
        for(String kafkaTopic : kafkaTopicsSplited) {
            topics.add(kafkaTopic);
        }//com.yonyou.logAnalysisModule.logAnalysis
        JavaPairInputDStream<String, String> logDStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);
		JavaDStream<String> map = logDStream.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) throws Exception {
				return tuple2._2;
			}
		});

		JavaDStream<String> words = map.flatMap(new FlatMapFunction<String, String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}

		});

		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}

		});

		// updateStateByKey,就可以实现直接通过spark维护一份每个单词的全局的统计次数
		JavaPairDStream<String, Integer> wordcounts = pairs.updateStateByKey(

				// 这里的Optional,相当于scala中的样例类,就是Option,可以理解它代表一个状态,可能之前存在,也可能之前不存在
				new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>(){

					private static final long serialVersionUID = 1L;

					// 实际上,对于每个单词,每次batch计算的时候,都会调用这个函数,第一个参数,values相当于这个batch中,这个key的新的值,
					// 可能有多个,比如一个hello,可能有2个1,(hello,1) (hello,1) 那么传入的是(1,1)
					// 那么第二个参数表示的是这个key之前的状态,其实泛型的参数是你自己指定的
					@Override
					public Optional<Integer> call(List<Integer> values,	Optional<Integer> state) throws Exception {
						// 定义一个全局的计数
						Integer newValue = 0;
						if(state.isPresent()){
							newValue = state.get();
						}
						for(Integer value : values){
							newValue += value;
						}
						return Optional.of(newValue);
					}
		});

		wordcounts.print();

		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
