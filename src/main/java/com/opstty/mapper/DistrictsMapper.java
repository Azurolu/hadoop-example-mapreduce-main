package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class DistrictsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

     // We filter line base on header value to skip header
     if(!value.toString().contains("ARRONDISSEMENT")) {
         Text arron = new Text(value.toString().split( ";")[1]);
         context.write(arron,NullWritable.get());
     }
    }

}
