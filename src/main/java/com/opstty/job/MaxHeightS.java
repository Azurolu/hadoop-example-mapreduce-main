package com.opstty.job;

import com.opstty.mapper.MaxHeightSMapper;
import com.opstty.reducer.MaxHeightSReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class MaxHeightS {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length <2){
            System.err.println("Usage: maxheightspecies <in> [<in>...] <out>");
            System.exit(2);

        }
        Job job= Job.getInstance(conf, "maxheightspecies");
        job.setJarByClass(MaxHeightS.class);
        job.setMapperClass(MaxHeightSMapper.class);
        job.setCombinerClass(MaxHeightSReducer.class);
        job.setReducerClass(MaxHeightSReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i =0; i <otherArgs.length -1; ++i){
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length -1]));
        new Path(otherArgs[otherArgs.length-1]);
        System.exit(job.waitForCompletion( true) ? 0:1);
    }
}