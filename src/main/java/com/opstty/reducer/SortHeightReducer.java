package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.*;

public class SortHeightReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private IntWritable idk = new IntWritable();

    public void reduce(IntWritable heightkey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        for (IntWritable val : values) {
            context.write(heightkey,val);
        }
    }
}


