package com.xy.hadoop.mapreduce;

import com.xy.hadoop.pojo.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 找出"tq"文件中的每个月排名前二的温度
 */
public class TemperatureMR {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 配置信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(TemperatureMR.class);
        job.setJobName("temperature_job");

        // 2 设置获取文件的HDFS路径和输出结果路径
        // 可以不用写输入的文件名！！！
        Path inPath = new Path("/input/temperature");
        FileInputFormat.addInputPath(job, inPath);

        Path outputPath = new Path("/output/temperature");
        FileSystem fileSystem = outputPath.getFileSystem(conf);
        if (fileSystem.exists(outputPath)) {
            // 如果存在该路径，递归删除
            fileSystem.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);


        // 3 设置Mapper类以及输入的key和value类型
        job.setMapperClass(TemperatureMapper.class);
        job.setMapOutputKeyClass(TemperatureDTO.class);
        job.setMapOutputValueClass(Text.class);

        // 4 设置自定义比较器
        job.setSortComparatorClass(TemperatureSortComparator.class);

        // 5 设置reducer分区器
        job.setPartitionerClass(TemperatureDPartitioner.class);

        // 6 自定义组排序器
        job.setGroupingComparatorClass(TemperatureGroupComparator.class);

        // 7 设置reducer数量
        job.setNumReduceTasks(2);

        // 8 设置reducer
        job.setReducerClass(TemperatureReducer.class);

        //9 submit

        job.waitForCompletion(true);
    }
}
