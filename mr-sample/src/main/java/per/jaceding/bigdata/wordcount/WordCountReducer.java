package per.jaceding.bigdata.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * WordCountReducer
 *
 * @author jaceding
 * @date 2020/7/4
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountReducer.class.getName());

    private int sum;
    private IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        LOG.info("key#" + key);
        // 1 累加求和
        sum = 0;
        for (IntWritable count : values) {
            LOG.info("count#" + count);
            sum += count.get();
        }

        // 2 输出
        v.set(sum);
        context.write(key, v);
    }
}
