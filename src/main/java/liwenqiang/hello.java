package liwenqiang;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;

public class hello {
    public static void main(String[] args) throws IOException {
        URI uri = URI.create ("hdfs://10.3.250.19:9000");
        Configuration conf = new Configuration();
//        conf.set("fs.default.name","hdfs://10.3.250.19:9000");
        FileSystem file = FileSystem.get(uri,conf);
        RemoteIterator<LocatedFileStatus> listFiles = file.listFiles(new Path("/foo"), false);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath());
        }
        System.out.println("done");
    }
}
