package wjt.demo;

/**
 * @description:
 * @author: wanjintao
 * @time: 2020/8/17 17:11
 */
public class ETLUtil {

    /**
     * 1. 过滤掉长度不够的，小于9个字段
     * 2.过滤类别字段中的空格
     * 3.修改相关视频字段的分割符，由'\t'替换为'&'
     *
     * @param oriStr 输入参数，原始数据
     * @return 过滤后的数据
     */

    public static String etlStr(String oriStr) {

        StringBuffer sb = new StringBuffer();

        //1. 切割
        String[] fields = oriStr.split("\t");

        //2. 对字段长度进行过滤
        if (fields.length < 9) {
            return null;
        }

        //3. 去掉类别字段中的空格
        fields[3] = fields[3].replaceAll(" ", "");

        //4. 修改相关视频字段的分割符，由'\t'替换为'&'
        for (int i = 0; i < fields.length; i++) {

            //对相关ID进行处理
            if (i < 9) {
                if (i == fields.length - 1) {
                    sb.append(fields[i]);
                }
                else {
                    sb.append(fields[i]).append("\t");
                }
            }
            else {
                //对相关字段进行处理
                if (i == fields.length - 1) {
                    sb.append(fields[i]);
                }
                else {
                    sb.append(fields[i]).append("&");
                }
            }

        }

        //5. 返回结果
        return sb.toString();
    }

//    public static void main(String[] args) {
//
//        System.out.println(ETLUtil.etlStr("SDNkMu8ZT68\tw00dy911\t630\tPeople & Blogs\t186\t10181\t3.49\t494\t257\trjnbgpPJUks"));
//
//    }

}
