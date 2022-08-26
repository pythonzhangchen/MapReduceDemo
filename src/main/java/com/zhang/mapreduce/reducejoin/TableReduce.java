package com.zhang.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReduce extends Reducer<Text,TableBean,TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        // 准备初始化集合
        ArrayList<TableBean> orderBeans =new ArrayList<>();
        TableBean pdBean=new TableBean();

        for (TableBean value : values) {
                if("order".equals(value.getFlag())){ // 订单表
                    TableBean tempTableBean = new TableBean();

                    try {
                        BeanUtils.copyProperties(tempTableBean,value);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    orderBeans.add(tempTableBean);
                }else { //商品表
                    try {
                        BeanUtils.copyProperties(pdBean,value);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
        }
        //  循环遍历orderBeans,复制pdname
        for (TableBean orderBean : orderBeans) {
            orderBean.setPname(pdBean.getPname());
            context.write(orderBean,NullWritable.get());
        }
    }
}
