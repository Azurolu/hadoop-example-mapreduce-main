package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxHeightSReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable maxheightspecies = new IntWritable();

    public void reduce(Text speciesheightKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int max = 0;

        for (IntWritable valspe : values) {
            if (valspe.get() > max) {
                max = valspe.get();
            }
        }
        maxheightspecies.set(max);
        context.write(speciesheightKey, maxheightspecies);

    }


}
