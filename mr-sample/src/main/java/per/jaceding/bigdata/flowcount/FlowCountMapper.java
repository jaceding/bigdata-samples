package per.jaceding.bigdata.flowcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FlowCountMapper
 *
 * @author jaceding
 * @date 2020/7/4
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private static final Logger LOG = LoggerFactory.getLogger(FlowCountMapper.class.getName());

    private FlowBean v = new FlowBean();
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        LOG.info("key#" + key.get() + "value#" + value.toString());
        // 1 获取一行
        String line = value.toString();

        // 2 切割字段
        String[] fields = line.split("\t");

        // 3 封装对象
        // 取出手机号码
        String phoneNum = fields[1];

        // 取出上行流量和下行流量
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        k.set(phoneNum);
        v.set(downFlow, upFlow);

        // 4 写出
        context.write(k, v);
    }
}
