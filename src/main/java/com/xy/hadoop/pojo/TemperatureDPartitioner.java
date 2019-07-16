package com.xy.hadoop.pojo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TemperatureDPartitioner extends Partitioner<TemperatureDTO, Text> {


    @Override
    public int getPartition(TemperatureDTO temperatureDTO, Text text, int numPartitions) {
        return temperatureDTO.getYear() % numPartitions;
    }
}
