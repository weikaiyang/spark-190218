package com.atguigu.bigdata.java;

import java.util.*;

public class TestCompute {

    public static void main(String[] args) throws Exception {

        List<String> stringList = new ArrayList<String>();
        List<String> stringList1 = new ArrayList<String>();

        stringList.add("Hello");
        stringList1.add("Scala");
        stringList.add("Kafka");
        stringList1.add("Oozie");

        Thread t1 = new MyThread(stringList);
        Thread t2 = new MyThread(stringList1);

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        System.out.println("主程序执行完毕");



        // h,s,k,o

        // 将集合中的每一个字符串的首写字母，变成小写并截取后打印
//        for ( String str : stringList ) {
//
//            //String s = str.toLowerCase().substring(0, 1);
//            //System.out.println(s);
//            String s = StringUtil.subString(StringUtil.toLowerCase(str), 0, 1);
//            System.out.println(s);
//        }

    }


}

class MyThread extends Thread {

    private List<String> stringList;

    public MyThread(List<String> stringList) {
        this.stringList = stringList;
    }


    public void run() {
        for ( String str : stringList ) {

            //String s = str.toLowerCase().substring(0, 1);
            //System.out.println(s);
            String s = StringUtil.subString(StringUtil.toLowerCase(str), 0, 1);
            System.out.println(s);
        }
    }


}

class StringUtil {

    public static String toLowerCase(String s) {
        return s.toLowerCase();
    }


    public static String subString( String s, int start, int end ) {
        int i = 10 / 0;
        return s.substring(start, end);
    }


}
// 计算单元 ： RDD, 封装计算逻辑，而不存储数据
class ComputeObj {

    private List list ;
    private List<ComputeObj> cos;

    // 将计算逻辑封装
//    public String compute() {
//
//        for (ComputeObj co : cos) {
//            co.compute(list);
//        }
//    }
}
