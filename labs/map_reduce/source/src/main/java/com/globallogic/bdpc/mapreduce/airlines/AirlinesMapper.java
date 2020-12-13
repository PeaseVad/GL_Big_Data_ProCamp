package com.globallogic.bdpc.mapreduce.airlines;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirlinesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable offset, Text lineText, Context context)
            throws IOException, InterruptedException {
        String line = lineText.toString();
        String airline = line.split(",")[4];
        int delay = 0;
        try {
            delay = Integer.parseInt(line.split(",")[11]);
        } catch (NumberFormatException nfe) {
            return;
        }

        context.write(new Text(airline), new IntWritable(delay));
    }
}
