package com.zhang.mapreduce.etl;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class WebLogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置

        args =new String[]{"C:/input/inputlog","C:/input/hadoop/output1"};

        // 1 获取配置信息，获取 job 对象实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2 指定本程序的jar包所在的本地路径
        job.setJarByClass(WebLogDriver.class);
        // 3 关联 Mapper/Reducer 业务处理类
        job.setMapperClass(WebLogMapper.class);
//      job.setReducerClass(TableReduce.class);
        // 4 指定Mapper输出数据的 key-value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 5 指定最终输出的数据的 key-value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        // Map端ETL的逻辑不需要Reduce阶段，设置reduceTask数量为0
        job.setNumReduceTasks(0);

        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 7 提交作业
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
