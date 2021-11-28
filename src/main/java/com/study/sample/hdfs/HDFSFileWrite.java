package com.study.sample.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class HDFSFileWrite {
    public static void main(String[] args) {
        if (args == null || args.length != 2) {
            System.out.println("使用方式：com.study.sample.hdfs.HDFSFileWrite [本地文件] [hdfs目标文件]");
            return;
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "hdfs://192.168.254.167:9820");
        try {
            FileSystem fs = FileSystem.get(conf);

            String inFilepath = args[0];
            Path targetFile = new Path(inFilepath);
            if (!fs.exists(targetFile)) {
                System.out.println("Output file already exists:" + inFilepath);
                throw new IOException("Output file already exists:" + inFilepath);
            }

            InputStream in = new BufferedInputStream(new FileInputStream(inFilepath));

            Path outFile = new Path(args[1]);
            FSDataOutputStream out = fs.create(outFile);

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
