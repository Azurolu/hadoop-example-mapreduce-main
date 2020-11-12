package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountSpeciesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable countspecies = new IntWritable();

    public void reduce(Text speciesKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int sum = 0;
        for (IntWritable valspe : values) {
            sum += valspe.get();
        }
        countspecies.set(sum);
        context.write(speciesKey, countspecies);
    }

}
