package WordCountMapper.wordCount;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class myMapper {
    public static class SampleMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable count = new IntWritable(1);
        private final Text text = new Text();

        @Override
        public void map(LongWritable longWritable, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                text.set(tokenizer.nextToken());
                outputCollector.collect(text, count);
            }
        }
    }

    public static class SampleReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }

            output.collect(key, new IntWritable(sum));
        }
    }

    private static class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {

        String country;
        String state;

        @Override
        public int compareTo(CompositeGroupKey pop) {
            if (pop == null) {
                return 0;
            }
            int inCnt = country.compareTo(pop.country);
            return inCnt == 0 ? state.compareTo(pop.state) : inCnt;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            WritableUtils.writeString(out, country);
            WritableUtils.writeString(out, state);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.country = WritableUtils.readString(dataInput);
            this.state = WritableUtils.readString(dataInput);
        }

        @Override
        public String toString() {
            return country.toString() + ":" + state.toString();
        }
    }
}


