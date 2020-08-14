package MySink;

import Template.DataStruct;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author 123456
 * @date 2020/4/15 17:53
 */
public class MongoSink extends RichSinkFunction<DataStruct> implements SinkFunction<DataStruct> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }



    @Override
    public void invoke(DataStruct value, Context context) throws Exception {
    //        for (DataStruct sinkstruct:value){
    //            System.out.println("我是sink,我拿到了数据sinkstruct，："+sinkstruct.ID+" "+sinkstruct.Name);
    //        }
        System.out.println("我是sink,我拿到了数据sinkstruct，："+value.ID+" "+value.Name);


    }


}
