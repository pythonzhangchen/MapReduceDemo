package com.zhang.mapreduce.outputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息，获取 job 对象实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2 指定本程序的jar包所在的本地路径
        job.setJarByClass(LogDriver.class);
        // 3 关联 Mapper/Reducer 业务处理类
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReduce.class);
        // 4 指定Mapper输出数据的 key-value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 5 指定最终输出的数据的 key-value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置自定义的outPutFormat
        job.setOutputFormatClass(LogOutputFormat.class);

        // 6 设置输入路径和输出路径本地模式
        FileInputFormat.setInputPaths(job, new Path("C:\\input\\inputoutputformat"));
        // 虽然我们自定义了outputformat,但是因为我们的outputformat继承来自fileoutputformat
        // 而fileoutputformat要输出一个_SUCCESS文件，所以在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path("C:\\input\\hadoop\\output231"));
        // 7 提交作业
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
