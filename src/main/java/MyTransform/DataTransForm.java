package MyTransform;

import Template.DataStruct;
import org.apache.flink.api.common.functions.MapFunction;



/**
 * @author 123456
 * @date 2020/4/17 10:59
 * 接口指定具体类型
 */
public class DataTransForm implements MapFunction<DataStruct, DataStruct> {
    public String NN;

    @Override
    public DataStruct map(DataStruct value) throws Exception {
        value.Name+="map 操作";
        return value;
    }
}
