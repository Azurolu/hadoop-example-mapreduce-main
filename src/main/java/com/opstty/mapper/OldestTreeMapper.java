package com.opstty.mapper;

import com.opstty.Informations;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<LongWritable, Text, IntWritable,Informations> {
    private final static IntWritable one = new IntWritable(1);

    private Informations agedist = new Informations();

    private IntWritable age = new IntWritable();
    private IntWritable district = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (!value.toString().contains("OBJECTID")) {
            IntWritable district = new IntWritable((int) Float.parseFloat(value.toString().split(";")[1]));
            Text age_str = new Text(value.toString().split(";")[5]);

            if (!age_str.toString().isEmpty()) {
                IntWritable age = new IntWritable((int) Float.parseFloat(value.toString().split(";")[5]));
                agedist.set(age, district);
                context.write(one, agedist);
            }
        }
    }
}