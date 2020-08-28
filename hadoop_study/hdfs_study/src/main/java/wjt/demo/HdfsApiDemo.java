package wjt.demo;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * @description:
 * @author: wanjintao
 * @time: 2020/8/5 16:10
 */
public class HdfsApiDemo {

    /*
       使用url方式访问数据
     */
    @Test
    public void urlhdfs() throws IOException {
        //1. 注册url
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        //2. 获取hdfs文件的输入流
        InputStream inputStream = new URL("hdfs://westgis181:9000/wjt/test001.txt").openStream();

        //3. 获取本地文件的输出流
        FileOutputStream outputStream = new FileOutputStream(new File("data/test007.txt"));

        //4. 实现文件的拷贝
        IOUtils.copy(inputStream, outputStream);

        //5. 关流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);

    }

    /*
       获取FileSystem的方式一
     */
    @Test
    public void getFileSystem1() throws IOException {
        //1:创建Configuration对象
        Configuration configuration = new Configuration();

        //2:设置文件系统的类型
        configuration.set("fs.defaultFS", "hdfs://westgis181:9000");

        //3:获取指定的文件系统
        FileSystem fileSystem = FileSystem.get(configuration);

        //4:输出
        System.out.println(fileSystem);
    }

    /*
       获取FileSystem的方式二
     */
    @Test
    public void getFileSystem2() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://westgis181:9000"), new Configuration());

        System.out.println(fileSystem);
    }

    /*
       获取FileSystem的方式三
     */
    @Test
    public void getFileSystem3() throws IOException {
        Configuration configuration = new Configuration();
        //指定文件系统类型
        configuration.set("fs.defaultFS", "hdfs://westgis181:9000");
        //获取指定的文件系统
        FileSystem fileSystem = FileSystem.newInstance(configuration);

        System.out.println(fileSystem);
    }

    /*
       获取FileSystem的方式四
     */
    @Test
    public void getFileSystem4() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://westgis181:9000"), new Configuration());

        System.out.println(fileSystem);

    }

    /*
       hdfs文件的遍历
     */
    @Test
    public void listFiles() throws URISyntaxException, IOException {

        //1:获取FileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://westgis181:9000"), new Configuration());

        //2:调用方法listFiles 获取 /目录下所有的文件信息
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/wjt"), true);

        //3:遍历迭代器
        while (iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();

            //获取文件的绝对路径 : hdfs://westgis181:9000/xxx
            System.out.println(fileStatus.getPath() + "----" +fileStatus.getPath().getName());

            //文件的block信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println("block数:"+blockLocations.length);
        }
    }

    /*
       hdfs创建文件夹
     */
    @Test
    public void mkdirsTest() throws URISyntaxException, IOException {
        //1:获取FileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://westgis181:9000"), new Configuration());

        //2:创建文件夹
        //boolean bl = fileSystem.mkdirs(new Path("/aaa/bbb/ccc"));
        fileSystem.create(new Path("/aaa/bbb/ccc/a.txt"));
        fileSystem.create(new Path("/aaa2/bbb/ccc/a.txt"));
        //System.out.println(bl);

        //3: 关闭FileSystem
        //fileSystem.close();

    }

    /*
       实现文件下载
     */
    @Test
    public void downloadFile() throws URISyntaxException, IOException {
        //1. 获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://westgis181:9000"), new Configuration());

        //2. 获取HDFS的输入流
        FSDataInputStream inputStream = fileSystem.open(new Path("/wjt/zookeeper.out"));

        //3. 获取本地路径的输出流
        FileOutputStream outputStream = new FileOutputStream("data/test003.txt");

        //4. 文件的拷贝
        IOUtils.copy(inputStream, outputStream);

        //5. 关闭流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
    }

    /*
       实现文件下载方式二
     */
    @Test
    public void downloadFile2() throws URISyntaxException, IOException {
        //1. 获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://westgis181:9000"), new Configuration());

        //2. 调用方法实现文件的下载
        fileSystem.copyToLocalFile(new Path("/wjt/test001.txt"),new Path("data/test004.txt"));

        //关闭FileSystem
        fileSystem.close();
    }

    /*
       实现文件上传
     */
    @Test
    public void uploadFile() throws URISyntaxException, IOException {
        //1. 获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://westgis181:9000"), new Configuration());

        //2. 调用方法实现文件的上传
        fileSystem.copyFromLocalFile(new Path("data/test004.txt"),new Path("/wjt/test002.txt"));

        //关闭FileSystem
        fileSystem.close();
    }

    /*
      实现文件的下载:方式2
     */
    @Test
    public void downloadFile3() throws URISyntaxException, IOException, InterruptedException {
        //1:获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://westgis181:9000"), new Configuration(),"root");
        //2:调用方法，实现文件的下载

        fileSystem.copyToLocalFile(new Path("/wjt/test001.txt"), new Path("data/test005.txt"));

        //3:关闭FileSystem
        fileSystem.close();
    }

    /*
       小文件的合并
     */
    @Test
    public void mergeFile() throws URISyntaxException, IOException, InterruptedException {
        //1:获取FileSystem（分布式文件系统）
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://westgis181:9000"), new Configuration(),"root");

        //2:获取hdfs大文件的输出流
        FSDataOutputStream outputStream = fileSystem.create(new Path("/big_txt.txt"));

        //3:获取一个本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());

        //4:获取本地文件夹下所有文件的详情
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("data/"));

        //5:遍历每个文件，获取每个文件的输入流
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath());

            //6:将小文件的数据复制到大文件
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
        }

        //7:关闭流
        IOUtils.closeQuietly(outputStream);
        localFileSystem.close();
        fileSystem.close();
    }


}
