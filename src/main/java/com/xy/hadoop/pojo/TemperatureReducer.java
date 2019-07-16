package com.xy.hadoop.pojo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TemperatureReducer extends Reducer<TemperatureDTO, Text, Text, IntWritable> {

    Text reducerKey = new Text();
    IntWritable reducerVal = new IntWritable();

    /**
     * 与Mapper的输出对应
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(TemperatureDTO key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int flg = 0;
        int day = 0;

        for (Text v : values) {

            if (flg == 0) {
                day = key.getDay();

                reducerKey.set(key.getThisDate().toString());
                reducerVal.set(key.getTemperature());
                context.write(reducerKey, reducerVal);
                flg++;

            }
            if (flg != 0 && day != key.getDay()) {

                reducerKey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
                reducerVal.set(key.getTemperature());
                context.write(reducerKey, reducerVal);
                return;
            }


        }

    }
}
