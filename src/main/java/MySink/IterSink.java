package MySink;

import Template.DataStruct;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.concurrent.TimeUnit;

/**
 * @author 123456
 * @date 2020/4/20 11:38
 */
public class IterSink  extends RichSinkFunction<Iterable<DataStruct>> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(Iterable<DataStruct> values, Context context) throws Exception {
        // 每次消费10个
        for (DataStruct dataStruct:values){
            System.out.println("迭代器Sink ，写数据 "+dataStruct.Name+"  "+dataStruct.ID
            );
        }
        TimeUnit.MILLISECONDS.sleep(1000);
    }
}
