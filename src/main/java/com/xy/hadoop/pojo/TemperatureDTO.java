package com.xy.hadoop.pojo;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;

@Data
public class TemperatureDTO implements WritableComparable<TemperatureDTO> {
    private int year;
    private int month;
    private int day;
    private int temperature;

    private LocalDate thisDate;

    public TemperatureDTO() {
        thisDate = LocalDate.of(year, month, day);
    }

    @Override
    public int compareTo(TemperatureDTO temperatureDTO) {

        LocalDate dtoDate = LocalDate.of(temperatureDTO.getYear(), temperatureDTO.getMonth(), temperatureDTO.getDay());

        return thisDate.compareTo(dtoDate);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(temperature);
    }

    /**
     * 读与写要一致
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.temperature = in.readInt();
    }
}
