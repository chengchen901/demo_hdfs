package com.study.sample.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;

public class HDFSFileRead {
    public static void main(String[] args) {
        if (args == null || args.length != 1) {
            System.out.println("使用方式：com.study.sample.hdfs.HDFSFileRead [hdfs目标文件]");
            return;
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "hdfs://192.168.254.167:9820");
        try {
            FileSystem fs = FileSystem.get(conf);

            String filepath = args[0];
            Path targetFile = new Path(filepath);
            if (!fs.exists(targetFile)) {
                System.out.println("Input file not found:" + filepath);
                throw new IOException("Input file not found:" + filepath);
            }

            FSDataInputStream in = fs.open(targetFile);
            OutputStream out = System.out;
            byte[] buffer = new byte[256];
            try {
                int byteRead = 0;
                while ((byteRead = in.read(buffer)) > 0) {
                    out.write(buffer, 0, byteRead);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                in.close();
                out.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
