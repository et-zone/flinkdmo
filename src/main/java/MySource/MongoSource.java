package MySource;

import Template.DataStruct;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * @author 123456
 * @date 2020/4/15 17:44
 */
// 生产一个扔出去一个---不需要扔出去一堆，但是可以被sink一堆处理
public class MongoSource extends RichSourceFunction<DataStruct> {
    private boolean canceled = false; //自定义循环条件
    private static final int interval = 100;//自定义停止时间
    private static Integer Count=1 ;
    @Override
    public void run(SourceContext ctx) throws Exception {
    while (!canceled){
//            DataStruct out = new DataStruct;
    DataStruct out = new DataStruct();
        out.ID=Count;
        out.Name="数据资源"+Count;
        System.out.println("我是Source,我生产了数据struct： ID= "+out.ID+" Name="+out.Name);
        ctx.collect(out);    // 生产一个扔出去一个
        TimeUnit.MILLISECONDS.sleep(interval);
        Count++;
    }
    }

    @Override
    public void cancel() {
        canceled=true;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }
}
