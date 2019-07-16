package com.xy.hadoop.hdfs;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

@Slf4j
public class HDFSTests {
        private Configuration conf = null;
        private FileSystem fs = null;


        @Before
        public void conn() throws Exception {

        /*conf = new Configuration(false);
        conf.set("fs.defaultFS", "hdfs://rhyme:9000");*/
            conf = new Configuration();
            fs = FileSystem.get(URI.create("hdfs://hadoop001:9000"), conf, "hadoop");

            //fs = FileSystem.get(conf);
        }

        @After
        public void close() throws Exception {
            fs.close();
        }

        @Test
        public void testConf() {
            System.out.println(conf.get("fs.defaultFS"));
            System.out.println(conf.get("hadoop.tmp.dir"));
        }

        @Test
        public void fileStatus() throws IOException {
            FileStatus[] statuses = fs.listStatus(new Path("/tmp"));
            for (FileStatus status : statuses) {
                log.info("{}", status.toString());
            }
        }

        @Test
        public void mkdir() throws Exception {

            Path dir = new Path("/yg/haha");
            if (!fs.exists(dir)) {
                System.out.println(fs.mkdirs(dir));
            }
        }

        @Test
        public void uploadFile() throws Exception {


            // HDFS目录
            Path file = new Path("/input/temperature/tq.txt");
            FSDataOutputStream output = fs.create(file);

            // 本地windows目录
            InputStream input = new BufferedInputStream(new FileInputStream(new File("H:\\study\\code\\hadoop-examples\\file\\tq")));

            IOUtils.copyBytes(input, output, conf, true);

        }

        @Test
        public void getBlockLocations() throws IOException {
            Path path = new Path("/yg/haha/hello.java");
            FileStatus fileStatus = fs.getFileStatus(path);
            BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

            for (int i = 0; i < blockLocations.length; i++) {
                log.info("getBlockLocations :{}", blockLocations[i]);
            }
        }

        @Test
        public void readFile() throws IOException {
            Path path = new Path("/yg/haha/hello.java");
            FSDataInputStream fsDataInputStream = this.fs.open(path);

            // 读取一个字节
            log.info("readFile : {}", (char) fsDataInputStream.readByte());//p
            log.info("readFile : {}", (char) fsDataInputStream.readByte());//u
            log.info("readFile : {}", (char) fsDataInputStream.readByte());//b
            log.info("readFile : {}", (char) fsDataInputStream.readByte());//l
            log.info("readFile : {}", (char) fsDataInputStream.readByte());//i
            log.info("readFile : {}", (char) fsDataInputStream.readByte());//c

        }

}
