package com.packetloop.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class HDFSClient {
    public static void main(String[] args) throws IOException {
        String file = args[0];
        Configuration configuration = new Configuration();
        FileSystem hdfs = FileSystem.get(configuration);
        Path path = new Path(file);
        FSDataInputStream fp = hdfs.open(path);

        ArrayList<String> arguments = new ArrayList<String>(Arrays.asList(args));
        arguments.remove(0);
        while (arguments.size() > 0) {
            int offset = Integer.parseInt(arguments.remove(0));
            if (arguments.size() == 0) {
                // stream forever, exit
                fp.seek(offset);
                while (true) {
                    int size = 1024;
                    byte[] buf = new byte[size];
                    int read = fp.read(buf);
                    if (read == -1)
                        return;
                    System.out.write(buf, 0, read);
                }
            } else {
                // continue as normal
                int size = Integer.parseInt(arguments.remove(0));
                byte[] buf = new byte[size];
                fp.seek(offset);
                int read = fp.read(buf);
                //System.err.println(read + " bytes read from " + file + ":" + offset);
                System.out.write(buf, 0, read);
            }
        }
    }
}

