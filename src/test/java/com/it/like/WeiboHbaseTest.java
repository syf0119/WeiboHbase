package com.it.like;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class WeiboHbaseTest {
    private static byte[] contentTable = Bytes.toBytes("weibo:content");
    private static byte[] relationTabale =Bytes.toBytes("weibo:relation");
    private static byte[] momentTable = Bytes.toBytes("weibo:moment");
    private static Connection connection;

    public void setUp() throws Exception {
        System.out.println("初始化");
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "spring:2181,summer:2181,autumn:2181");
        connection = ConnectionFactory.createConnection(configuration);
    }

    //   1) 创建命名空间以及表名的定义
    @Test
    public void createNamespaceAndTableNames() throws Exception {
        setUp();
        Admin admin = connection.getAdmin();
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("weibo").build();
        admin.createNamespace(namespaceDescriptor);

        admin.close();
        connection.close();


    }


    //2) 创建微博内容表
    @Test
    public void createContentTable() throws Exception {
        setUp();
        Admin admin = connection.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(contentTable));
        HColumnDescriptor info = new HColumnDescriptor("info");
        info.setMaxVersions(1);
        info.setMinVersions(1);
        info.setBlockCacheEnabled(true);
        info.setBlocksize(2048 * 1024);
        hTableDescriptor.addFamily(info);

        admin.createTable(hTableDescriptor);
        admin.close();
        connection.close();
    }

    //3) 创建用户关系表
    @Test
    public void createRelationTable() throws Exception {
        setUp();
        Admin admin = connection.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(relationTabale));
        HColumnDescriptor attends = new HColumnDescriptor("attends");
        HColumnDescriptor followers = new HColumnDescriptor("followers");

        attends.setMaxVersions(1);
        attends.setMinVersions(1);
        attends.setBlockCacheEnabled(true);
        attends.setBlocksize(2048 * 1024);

        followers.setMaxVersions(1);
        followers.setMinVersions(1);
        followers.setBlockCacheEnabled(true);
        followers.setBlocksize(2048 * 1024);
        hTableDescriptor.addFamily(attends);
        hTableDescriptor.addFamily(followers);

        admin.createTable(hTableDescriptor);
        admin.close();
        connection.close();
    }

    //4) 创建用户微博内容接收邮件表
    @Test
    public void createMomentTable() throws Exception {
        setUp();
        Admin admin = connection.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(momentTable));
        HColumnDescriptor info = new HColumnDescriptor("info");
        info.setMaxVersions(1000);
        info.setMinVersions(1000);
        info.setBlockCacheEnabled(true);
        info.setBlocksize(2048 * 1024);
        hTableDescriptor.addFamily(info);

        admin.createTable(hTableDescriptor);
        admin.close();
        connection.close();
    }

    @Test
    public void post() throws Exception {
        String uid ;
        String post;

        setUp();
        //把发布的内容放入content表中
        uid = "0002";
        post = "two is one add zero plus two";
        Table content = connection.getTable(TableName.valueOf(contentTable));
        String RowkeyString = uid + System.currentTimeMillis();
        Put putContent = new Put(RowkeyString.getBytes());
        putContent.addColumn("info".getBytes(), "text".getBytes(), post.getBytes());
        content.put(putContent);
        //通过关系表获取uid的粉丝，再把post放入粉丝的moment里

        Table relation = connection.getTable(TableName.valueOf(relationTabale));
        Table moment = connection.getTable(TableName.valueOf(momentTable));
        Get get = new Get(uid.getBytes());
        Result result = relation.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            if ("followers".equals(CellUtil.cloneFamily(cell))) {
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                Put putMoment = new Put(qualifier);
                putMoment.add("info".getBytes(), uid.getBytes(), RowkeyString.getBytes());
                moment.put(putMoment);

            }
        }
        relation.close();
        moment.close();
        content.close();

        connection.close();
    }

    //6) 添加关注用户
    @Test
    public void follow() throws Exception {

        String uuid;
        String other;
        uuid="0001";
        other="0002";
        setUp();
        //在uuid的关系表中的attends列族中添加other
        Table relation = connection.getTable(TableName.valueOf(relationTabale));
        Table moment = connection.getTable(TableName.valueOf(momentTable));
        Table content = connection.getTable(TableName.valueOf(contentTable));
        Put putRelation1 = new Put(uuid.getBytes());
        putRelation1.addColumn("attends".getBytes(), other.getBytes(), other.getBytes());
        relation.put(putRelation1);

        Put putRelation2=new Put(other.getBytes());
        putRelation2.addColumn("followers".getBytes(), uuid.getBytes(), uuid.getBytes());
        relation.put(putRelation2);
        //other的post添加到uuid的moment中
        List<Put> list = new ArrayList<>();


        PrefixFilter prefixFilter = new PrefixFilter(other.getBytes());
        Scan scan = new Scan();
        scan.setFilter(prefixFilter);
        ResultScanner results = content.getScanner(scan);
        for (Result result : results) {
            byte[] row = result.getRow();
            Put put = new Put(uuid.getBytes());
            put.add("info".getBytes(), other.getBytes(),Long.parseLong(Bytes.toString(row).substring(other.length())), row);
            list.add(put);

        }
        moment.put(list);
        relation.close();
        moment.close();
        content.close();
        connection.close();


    }

    //7) 移除（取关）用户
    @Test
    public void disFollow() throws Exception {
        String uuid;

        String other;
        uuid="0001";
        other="0002";
        setUp();
        Table relation = connection.getTable(TableName.valueOf(relationTabale));
        Table moment = connection.getTable(TableName.valueOf(momentTable));
        //relation里 attends列族里删除other

        Delete deleteRelation = new Delete(uuid.getBytes());
        deleteRelation.addColumn("attends".getBytes(), other.getBytes());
        relation.delete(deleteRelation);


        Delete deleteRelation1=new Delete(other.getBytes());
        deleteRelation1.addColumn("followers".getBytes(), uuid.getBytes());
        relation.delete(deleteRelation1);
        //在moment表里删除所有other的post
        Delete deleteMoment = new Delete(uuid.getBytes());
        deleteMoment.addColumn("info".getBytes(), other.getBytes());
        moment.delete(deleteMoment);

        relation.close();
        moment.close();
        connection.close();


    }

    //8) 获取关注的人的微博内容
    @Test
    public void getPosts() throws Exception {
        String uuid;
        uuid="0001";
        setUp();
        Table moment = connection.getTable(TableName.valueOf(momentTable));
        Table content = connection.getTable(TableName.valueOf(contentTable));

        //通过moment表获取rowkey列表
        Get get = new Get(uuid.getBytes());
        get.addFamily("info".getBytes());
        get.setMaxVersions(4);
        Result momentResult = moment.get(get);
        Cell[] cells = momentResult.rawCells();
        //通过content表获取内容
        for (Cell cell : cells) {
            byte[] bytes = CellUtil.cloneValue(cell);
            Result contentResult = content.get(new Get(bytes));
            Cell[] contentCells = contentResult.rawCells();
            for (Cell contentCell : contentCells) {
                byte[] cloneValue = CellUtil.cloneValue(contentCell);
                System.out.println(Bytes.toString(cloneValue));
            }


        }
        moment.close();
        content.close();
        connection.close();
    }

    @Test
    public void practice() {
        System.out.println("12345".substring(1,3));
    }
}