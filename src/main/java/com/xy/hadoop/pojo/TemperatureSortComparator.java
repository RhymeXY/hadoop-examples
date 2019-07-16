package com.xy.hadoop.pojo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TemperatureSortComparator extends WritableComparator {
    public TemperatureSortComparator() {
        super(TemperatureDTO.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TemperatureDTO t1 = (TemperatureDTO) a;
        TemperatureDTO t2 = (TemperatureDTO) b;

        // 选出同年同月的前两个温度最大值
        int c1 = Integer.compare(t1.getYear(), t2.getYear());
        if (c1 == 0) {
            // 年月升序
            int c2 = Integer.compare(t1.getMonth(), t2.getMonth());
            if (c2 == 0) {
                // 温度降序
                return -Integer.compare(t1.getTemperature(), t2.getTemperature());
            }
            return c2;
        }

        return c1;
    }
}
