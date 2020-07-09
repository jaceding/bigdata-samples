package per.jaceding.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * hdfs 客户端
 *
 * @author jaceding
 * @date 2020/7/2
 */
public class HDFSClientTests {

    /**
     * 测试创建目录
     */
    @Test
    void testMkdirs() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), configuration, "root");
        // 2 创建目录
        fs.mkdirs(new Path("/sanguo/weiguo"));
        // 3 关闭资源
        fs.close();
    }

    /**
     * 测试文件上传
     * 参数优先级排序：（1）客户端代码中设置的值 >（2）ClassPath下的用户自定义配置文件 >（3）然后是服务器的默认配置
     */
    @Test
    void testCopyFromLocalFile() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9820"), configuration, "root");
        // 2 上传文件
//        fs.copyFromLocalFile(new Path("E:\\Download\\Xunlei\\hadoop-3.1.3-src.tar.gz"), new Path("/bad-hadoop-3.1.3-src.tar.gz"));
        fs.copyFromLocalFile(new Path("C:\\Users\\93197\\Desktop\\wc.txt"), new Path("/wordcount/input/wc.txt"));
        // 3 关闭资源
        fs.close();
    }

    /**
     * 测试文件下载
     */
    @Test
    void testCopyToLocalFile() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), configuration, "root");
        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false,
                new Path("/bad-hadoop-3.1.3-src.tar.gz"),
                new Path("E:\\Download\\Xunlei\\bad-hadoop-3.1.3-src.tar.gz"),
                true);
        // 3 关闭资源
        fs.close();
    }

    /**
     * 测试文件删除
     */
    @Test
    void testDelete() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), configuration, "root");
        // 2 执行删除
        fs.delete(new Path("/bad-hadoop-3.1.3-src.tar.gz"), true);
        // 3 关闭资源
        fs.close();
    }

    /**
     * 测试修改文件名
     */
    @Test
    void testRename() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), configuration, "root");
        // 2 修改文件名称
        fs.rename(new Path("/bad-hadoop-3.1.3-src.tar.gz"), new Path("/bad1-hadoop-3.1.3-src.tar.gz"));
        // 3 关闭资源
        fs.close();
    }

    /**
     * 测试获取文件详情
     */
    @Test
    void testListFiles() throws Exception {
        // 1获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), configuration, "root");

        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();
            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());
            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("-----------班长的分割线----------");
        }
        // 3 关闭资源
        fs.close();
    }

    /**
     * 判断文件和文件夹
     */
    @Test
    void testListStatus() throws Exception {
        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), configuration, "root");
        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/sanguo"));

        for (FileStatus fileStatus : listStatus) {
            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }

        // 3 关闭资源
        fs.close();
    }

    /**
     * 文件上传
     */
    @Test
    void putFileToHDFS() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), configuration, "root");
        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("E:\\Download\\Xunlei\\winutils-master.zip"));

        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/winutils-master.zip"));

        // 4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * 文件下载
     */
    @Test
    void getFileFromHDFS() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), configuration, "root");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/winutils-master.zip"));

        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("E:\\Download\\Xunlei\\winutils-master1.zip"));

        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    // 需求：分块读取HDFS上的大文件，比如根目录下的/winutils-master.zip

    /**
     * 下载第一块
     */
    @Test
    void readFileSeek1() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), configuration, "root");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/winutils-master.zip"));

        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("E:\\Download\\Xunlei\\winutils-master2.zip.part1"));

        // 4 流的拷贝
        byte[] buf = new byte[1024];

        for (int i = 0; i < 1024 * 128; i++) {
            fis.read(buf);
            fos.write(buf);
        }

        // 5关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    /**
     * 下载第二块
     */
    @Test
    void readFileSeek2() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), configuration, "root");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/winutils-master.zip"));

        // 3 定位输入数据位置
        fis.seek(1024*1024*128);

        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("E:\\Download\\Xunlei\\winutils-master2.zip.part2"));

        // 5 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 6 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    /**
     * 在Window命令窗口中进入到目录E:\Download\Xunlei\，然后执行如下命令，对数据进行合并
     * type winutils-master2.zip.part2 >> winutils-master2.zip.part1
     * 合并完成后，将winutils-master2.zip.part1重新命名为winutils-master2.zip。解压发现该包非常完整。
     */

    @BeforeAll
    public static void init() {
    }

    @AfterAll
    public static void destroy() {

    }
}
