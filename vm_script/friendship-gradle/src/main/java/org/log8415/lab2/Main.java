package org.log8415.lab2;

import java.io.IOException;
import java.util.*;

import com.google.common.collect.Collections2;
import com.google.common.collect.Collections2.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Main {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, String[], IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable bigNegative = new IntWritable(-9999999);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<String[], IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString(); // 0 TAB 1,2,3,4
            String[] parsedLine = line.split("[\\t,]");
            if(parsedLine.length > 1) {
                String personNumber = parsedLine[0];
                for(int i = 1; i < parsedLine.length; i++)
                {
                    String[] pair = {personNumber, parsedLine[i]};
                    output.collect(pair,bigNegative); // Should stay negative to avoid recommending a friend we already have
                }
                Combination.getCombinations(); // TODO, get all combinations of 2 items from the list minus the first number and output.collect(pair, one). Ex: output.collect([1,2],1);

            }

            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Main.class);
        conf.setJobName("frienships");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }


}
/* This code is contributed by Devesh Agrawal */
class Combination {

    /* arr[]  ---> Input Array
    data[] ---> Temporary array to store current combination
    start & end ---> Staring and Ending indexes in arr[]
    index  ---> Current index in data[]
    r ---> Size of a combination to be printed */
    static void combinationUtil(String arr[], int n, int r, int index,
                                String data[], int i)
    {
        // Current combination is ready to be printed, print it
        if (index == r)
        {
            for (int j=0; j<r; j++)
                System.out.print(data[j]+" ");
            System.out.println("");
            return;
        }

        // When no more elements are there to put in data[]
        if (i >= n)
            return;

        // current is included, put next at next location
        data[index] = arr[i];
        combinationUtil(arr, n, r, index+1, data, i+1);

        // current is excluded, replace it with next (Note that
        // i+1 is passed, but index is not changed)
        combinationUtil(arr, n, r, index, data, i+1);
    }

    // The main function that prints all combinations of size r
    // in arr[] of size n. This function mainly uses combinationUtil()
    static void getCombinations(String arr[], int n, int r)
    {
        // A temporary array to store all combination one by one
        String[] data = new String[r];

        // Print all combination using temporary array 'data[]'
        combinationUtil(arr, n, r, 0, data, 0);
    }
}
