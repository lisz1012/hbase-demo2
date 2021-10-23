package com.lisz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

// 对HBase做SQL查询可以用Apache Phoenix
public class HBaseDemo {
	private Configuration conf = null;
	private Connection connection = null;
	//表的管理对象：
	private Admin admin;
	//创建表的对象
	private TableName tableName = TableName.valueOf("phone");

	@Before
	public void init() throws IOException {
		// 创建配置文件对象
		conf = HBaseConfiguration.create();
		//加载zk配置
		conf.set("hbase.zookeeper.quorum", "hadoop-02,hadoop-03,hadoop-04");
		//获取连接
		connection = ConnectionFactory.createConnection(conf);
		// 获取Admin对象
		admin = connection.getAdmin();
	}

	@Test
	public void createTable() throws IOException {
		// 定义表描述对象
		TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
		// 定义列祖描述对象
		ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("cf".getBytes());
		// 添加列族信息
		tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
		if (admin.tableExists(tableName)) {
			// 跟在命令行中一样，删除表之前先要disable它
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		// 创建表
		admin.createTable(tableDescriptorBuilder.build());
	}

	@After
	public void destroy() {
		try {
			admin.close();
			connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
