package liwenqiang;

import WordCountMapper.wordCount.myMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.net.URI;

public class hello {
    public static void main(String[] args) throws IOException {
//        URI uri = URI.create ("hdfs://10.3.250.19:9000");
//        Configuration conf = new Configuration();
////        conf.set("fs.default.name","hdfs://10.3.250.19:9000");
//        FileSystem file = FileSystem.get(uri,conf);
//        RemoteIterator<LocatedFileStatus> listFiles = file.listFiles(new Path("/foo"), false);
//        while (listFiles.hasNext()) {
//            LocatedFileStatus fileStatus = listFiles.next();
//            System.out.println(fileStatus.getPath());
//        }
//        System.out.println("done");
        String inputPath=args[0];
        String outputPath=args[1];
        JobConf conf = new JobConf(myMapper.class);
        conf.setJobName("SampleMapReduce");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(myMapper.SampleMapper.class);
        conf.setCombinerClass(myMapper.SampleReducer.class);
        conf.setReducerClass(myMapper.SampleReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new
                Path(inputPath)); FileOutputFormat.setOutputPath(conf, new
                Path(outputPath));
        JobClient.runJob(conf);
    }
}
