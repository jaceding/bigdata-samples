package per.jaceding.bigdata.flowcount;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FlowCountReducer
 *
 * @author jaceding
 * @date 2020/7/4
 */
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private static final Logger LOG = LoggerFactory.getLogger(FlowCountReducer.class.getName());

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        LOG.info("key#" + key);

        long sum_upFlow = 0;
        long sum_downFlow = 0;

        // 1 遍历所用bean，将其中的上行流量，下行流量分别累加
        for (FlowBean flowBean : values) {
            LOG.info("upflow#" + flowBean.getUpFlow() + ",downflow#" + flowBean.getDownFlow());
            sum_upFlow += flowBean.getUpFlow();
            sum_downFlow += flowBean.getDownFlow();
        }

        // 2 封装对象
        FlowBean resultBean = new FlowBean(sum_upFlow, sum_downFlow);

        // 3 写出
        context.write(key, resultBean);
    }
}
