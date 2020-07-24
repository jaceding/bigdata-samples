package per.jaceding.bigdata.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * WordCountMapper
 *
 * @author jaceding
 * @date 2020/7/4
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountMapper.class.getName());

    private Text k = new Text();
    private IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        LOG.info("key#" + key + ",value#" + value.toString());
        // 1 获取一行
        String line = value.toString();

        // 2 切割
        String[] words = line.split(" ");

        // 3 输出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
