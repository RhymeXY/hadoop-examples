package com.xy.hadoop.pojo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TemperatureGroupComparator extends WritableComparator {
    public TemperatureGroupComparator() {
        super(TemperatureDTO.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TemperatureDTO t1 = (TemperatureDTO)a;
        TemperatureDTO t2 = (TemperatureDTO)b;


        int c1=Integer.compare(t1.getYear(), t2.getYear());
        if(c1==0){
            // 年月相等的合并到一组
            return Integer.compare(t1.getMonth(), t2.getMonth());
        }
        return c1;

    }

}
