package per.jaceding.bigdata.kvtext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author jaceding
 * @date 2020/7/7
 */
public class KvTextDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"./mr-sample/src/main/java/per/jaceding/bigdata/kvtext/input",
                "./mr-sample/src/main/java/per/jaceding/bigdata/kvtext/output"};

        Configuration conf = new Configuration();
        // 设置切割符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPARATOR, " ");
        // 1 获取job对象
        Job job = Job.getInstance(conf);

        // 2 设置jar包位置，关联mapper和reducer
        job.setJarByClass(KvTextDriver.class);
        job.setMapperClass(KvTextMapper.class);
        job.setReducerClass(KvTextReducer.class);

        // 3 设置map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 4 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 5 设置输入输出数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 6 设置输出数据路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 提交job
        job.waitForCompletion(true);
    }
}
