package com.zhang.mapreduce.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
                // 1 获取配置信息，获取 job 对象实例
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf);
                // 2 指定本程序的jar包所在的本地路径
                job.setJarByClass(MapJoinDriver.class);
                // 3 关联 Mapper/Reducer 业务处理类
                job.setMapperClass(MapJoinMapper.class);
//                job.setReducerClass(TableReduce.class);
                // 4 指定Mapper输出数据的 key-value 类型
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(NullWritable.class);
                // 5 指定最终输出的数据的 key-value 类型
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(NullWritable.class);

                // 加载缓存数据
                job.addCacheFile(new URI("file:///C:/input/tablecache/pd.txt"));
                // Map端Join的逻辑不需要Reduce阶段，设置reduceTask数量为0
                job.setNumReduceTasks(0);

                // 6 设置输入路径和输出路径
                FileInputFormat.setInputPaths(job, new Path("C:\\input\\inputtable2"));
                FileOutputFormat.setOutputPath(job, new Path("C:\\input\\hadoop\\output"));
                // 7 提交作业
                boolean result = job.waitForCompletion(true);

                System.exit(result ? 0 : 1);
            }
        }


