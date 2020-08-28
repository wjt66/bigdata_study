package wjt.demo;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @description:
 * @author: wanjintao
 * @time: 2020/8/15 9:54
 */
public class MYUDF extends UDF {

    public int evaluate(int data) {

        return data + 5;
    }

}
