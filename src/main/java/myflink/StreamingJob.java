
package myflink;

import MySink.IterSink;
import MySource.MongoSource;
import MyTransform.DataTransForm;
import SysConfig.CountTriggerWithTimeout;
import Template.DataStruct;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class StreamingJob {
//    //map 写法1
//    public static void main(String[] args) throws Exception {
//        // set up the streaming execution environment
//        MongoSink mongoSink = new MongoSink();
//        MongoSource mongoSource = new MongoSource();
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.addSource(mongoSource)
//                .name("MongoSource")
//                .map(new MapFunction<DataStruct, DataStruct>() {
//                    @Override
//                    public DataStruct map(DataStruct doc) {
//                        doc.Name+="map 操作";
//                        return doc;
//                    }
//                })
//                .addSink(mongoSink)
//                .setParallelism(1)
//                .name("mongoSink");
//
//        // execute program
//        env.execute("Flink Streaming Java API Skeleton");
//    }

//    //map 写法2
//    public static void main(String[] args) throws Exception {
//        // set up the streaming execution environment
//        MongoSink mongoSink = new MongoSink();
//        MongoSource mongoSource = new MongoSource();
//        DataTransForm mytransform = new DataTransForm();//只要类重写了map方法即可调用
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.addSource(mongoSource)
//                .name("MongoSource")
//                .map(mytransform)
//                .addSink(mongoSink)
//                .setParallelism(1)
//                .name("mongoSink");
//
//        // execute program
//        env.execute("Flink Streaming Java API Skeleton");
//    }



    //加window窗口
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
//        MongoSink mongoSink = new MongoSink();
        IterSink iterSink = new IterSink();
        MongoSource mongoSource = new MongoSource();
        DataTransForm mytransform = new DataTransForm();//只要类重写了map方法即可调用
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(mongoSource)
                .name("MongoSource")
                .map(mytransform)
                .timeWindowAll(Time.seconds(5))
                .trigger(
                        new CountTriggerWithTimeout<>(10, TimeCharacteristic.ProcessingTime)
                ).apply(
                        //这里用迭代器
                new AllWindowFunction<DataStruct, Iterable<DataStruct>, TimeWindow>() {
                    //---------------------------迭代器--------------------------原单个数据转为迭代器处理（个数为上面规定的10个）--------------
                    @Override
                    public void apply(TimeWindow window, Iterable<DataStruct> values, Collector<Iterable<DataStruct>> out) throws Exception {
                        out.collect(values);
                    }
                }
        ).name("")
                .addSink(iterSink)
                .setParallelism(1)
                .name("mongoSink");

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
