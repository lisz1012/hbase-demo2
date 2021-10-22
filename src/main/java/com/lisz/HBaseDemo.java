package com.lisz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

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