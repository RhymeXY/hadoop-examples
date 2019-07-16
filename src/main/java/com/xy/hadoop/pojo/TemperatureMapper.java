package com.xy.hadoop.pojo;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.time.LocalDate;

@Slf4j
public class TemperatureMapper extends Mapper<LongWritable, Text, TemperatureDTO, IntWritable> {

    private TemperatureDTO mapperOutputKey = new TemperatureDTO();
    private IntWritable mapperOutputValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //value:  1949-10-01 14:21:02	34c  >>  TemperatureDTO
        //时间与温度之间是一个tab，文本文件里看起来不明显
        String[] strs = StringUtils.split(value.toString(), '\t');
        log.info("TemperatureMapper map strs : {}", strs);

        LocalDate localDate = LocalDate.parse(strs[0]);

        mapperOutputKey.setYear(localDate.getYear());
        mapperOutputKey.setMonth(localDate.getMonthValue());
        mapperOutputKey.setDay(localDate.getDayOfMonth());

        // 去除c
        int temperature = Integer.parseInt(strs[1].substring(0, strs[2].lastIndexOf("c")));
        mapperOutputKey.setTemperature(temperature);

        mapperOutputValue.set(temperature);

        // 写给reducer的数据
        context.write(mapperOutputKey, mapperOutputValue);

    }
}
