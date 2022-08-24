package com.zhang.mapreduce.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/***
 * 1、 定义实现writable接 口
 * 2、重写序列化和反序列化方法
 * 3、重写空参构造
 * 4、toString方法
 */
public class FlowBean implements Writable {
    private long upFlow; // 上行流量
    private long downFlow; //下行流量
    private long sumFlow; //总流量

    public FlowBean(){

    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.upFlow = input.readLong();
        this.downFlow = input.readLong();
        this.sumFlow = input.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow +"\t"+ sumFlow;
    }
}
