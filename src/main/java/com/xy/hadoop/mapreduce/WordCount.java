package com.xy.hadoop.mapreduce;

import com.xy.hadoop.pojo.WordCountMapper;
import com.xy.hadoop.pojo.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // 创建一个job
        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCount.class);

        job.setJobName("wordCountJob");

        // 设置需要处理的文件路径
        Path inPath = new Path("/yg/haha/hello.txt");
        FileInputFormat.addInputPath(job, inPath);

        // 设置job完成后输出的文件路径
        Path outputPath = new Path("/output/wordcount");
        if (outputPath.getFileSystem(conf).exists(outputPath)) {
            // 路径如果先存在就删除
            outputPath.getFileSystem(conf).delete(outputPath, true);
        }

        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(WordCountMapper.class);
        // 设置Map处理后，输出的数据类型，reducer用来进行反序列化
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(WordCountReducer.class);

        job.waitForCompletion(true);

    }

}
