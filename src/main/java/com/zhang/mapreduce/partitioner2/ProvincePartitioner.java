package com.zhang.mapreduce.partitioner2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        //  text手机号
        String phone = text.toString();
        String prephone = phone.substring(0, 3);
        int partitioner;

        if ("136".equals(prephone)) {
            partitioner = 0;
        } else if ("137".equals(prephone)) {
            partitioner = 1;
        } else if ("138".equals(prephone)) {
            partitioner = 2;
        } else if ("139".equals(prephone)) {
            partitioner = 3;
        } else {
            partitioner = 4;
        }
        return partitioner;
    }
}
