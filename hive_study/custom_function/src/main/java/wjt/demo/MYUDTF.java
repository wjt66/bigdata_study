package wjt.demo;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @description:
 * @author: wanjintao
 * @time: 2020/8/15 11:19
 */
public class MYUDTF extends GenericUDTF {

    private List<String> dataList = new ArrayList<String>();

    //定义输出数据的列名和输出数据类型
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        //定义输出数据的列名
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("word");

        //定义输出数据的类型
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }
    
    public void process(Object[] objects) throws HiveException {

        //1. 获取数据
        String data = objects[0].toString();

        //2. 获取分隔符
        String splictkey = objects[1].toString();

        //3. 切分数据
        String[] words = data.split(splictkey);

        //4. 遍历写出
        for (String word : words) {

            //5. 将数据放置集合
            dataList.clear();
            dataList.add(word);

            //6. 写出数据的操作
            forward(dataList);

        }

    }

    public void close() throws HiveException {

    }
}
