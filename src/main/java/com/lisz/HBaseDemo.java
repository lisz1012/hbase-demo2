package com.lisz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

// 对HBase做SQL查询可以用Apache Phoenix. 操作表级别的时候用admin，操作数据的时候用table
public class HBaseDemo {
	private Configuration conf = null;
	private Connection connection = null;
	//表的管理对象：
	private Admin admin;
	//创建表的对象
	private TableName tableName = TableName.valueOf("phone");
	//Table 对象
	private Table table;
	private Random random = new Random();
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

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
		table = connection.getTable(tableName);
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

	@Test
	public void insert() throws IOException {
		Put put = new Put(Bytes.toBytes("222"));
		// 指定列族、列、值
		put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
		put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes("350"));
		put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sex"), Bytes.toBytes("female"));
		table.put(put);
	}

	/*
	  通过Get获取数据.
	 */
	@Test
	public void get() throws Exception {
		Get get = new Get(Bytes.toBytes("111"));
		// 在服务端坐数据过滤，挑选出符合需求的列
		get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"));
		get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"));
		get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sex"));
		final Result result = table.get(get);
		Cell cell = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("name"));
		System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
		cell = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("age"));
		System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
		cell = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("sex"));
		System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
	}

	// 获取表中所有的记录。blockcache里面划分了好几个区域：single、Multi、永久区
	@Test
	public void scan() throws Exception {
		Scan scan = new Scan();

		final ResultScanner scanner = table.getScanner(scan);
		for (Result res : scanner) {
			Cell cell = res.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("name"));
			System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
			cell = res.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("age"));
			System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
			cell = res.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("sex"));
			System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
		}
	}

	// 假设有10各用户，每个用户一年产生了10000条记录
	@Test
	public void insertBulk() throws Exception {
		List<Put> puts = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			String num = getNum("158");
			for (int j = 0; j < 10000; j++) {
				String dnum = getNum("177");
				String length = String.valueOf(random.nextInt(100));
				String date = getDate("2021");
				String type = String.valueOf(random.nextInt(2));
				String rowKey = num + "_" + (Long.MAX_VALUE - sdf.parse(date).getTime());
				Put put = new Put(Bytes.toBytes(rowKey));
				put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("dnum"), Bytes.toBytes(dnum));
				put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("length"), Bytes.toBytes(length));
				put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("date"), Bytes.toBytes(date));
				put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("type"), Bytes.toBytes(type));
				puts.add(put);
			}
		}
		table.put(puts);
	}

	private String getDate(String s) {
		return s + String.format("%02d%02d%02d%02d%02d",
				1 + random.nextInt(12), 1 + random.nextInt(31),
				random.nextInt(24), random.nextInt(60), random.nextInt(60));
	}

	private String getNum(String s) {
        return s + String.format("%08d", random.nextInt(99999999));
	}

	/*
		查询某个用户3月份的通话记录
	 */
	@Test
	public void scanByCondition() throws Exception {
		Scan scan = new Scan();
		// 这里容易出错，由于要按照时间的倒序存的，所以取的时候也要反过来规定范围
		String startRow = "15895088716_" + (Long.MAX_VALUE - sdf.parse("20210331235959").getTime());
		String stopRow = "15895088716_" + (Long.MAX_VALUE - sdf.parse("20210301000000").getTime());
		scan.withStartRow(Bytes.toBytes(startRow));
		scan.withStopRow(Bytes.toBytes(stopRow));
		final ResultScanner scanner = table.getScanner(scan);
		for (Result res : scanner) {
			Cell cell = res.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("dnum"));
			System.out.print(Bytes.toString(CellUtil.cloneValue(cell)) + "--");
			cell = res.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("length"));
			System.out.print(Bytes.toString(CellUtil.cloneValue(cell)) + "--");
			cell = res.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("date"));
			System.out.print(Bytes.toString(CellUtil.cloneValue(cell)) + "--");
			cell = res.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("type"));
			System.out.print(Bytes.toString(CellUtil.cloneValue(cell)));
			System.out.println();
		}
	}

	/**
	 * 查询某个用户所有的主叫记录（type=1）
	 * 1。某个用户
	 * 2。type=1
	 */
	@Test
	public void getByType() throws Exception {
		Scan scan = new Scan();
		// 创建过滤器的集合
		FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		// 创建过滤器
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("type"),
				CompareOperator.EQUAL, Bytes.toBytes("1"));
		filters.addFilter(filter1);
		// 前缀过滤器
		PrefixFilter filter2 = new PrefixFilter(Bytes.toBytes("15895088716"));
		filters.addFilter(filter2);
		scan.setFilter(filters);
		final ResultScanner scanner = table.getScanner(scan);
		for (Result res : scanner) {
			Cell cell = res.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("dnum"));
			System.out.print(Bytes.toString(CellUtil.cloneValue(cell)) + "--");
			cell = res.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("length"));
			System.out.print(Bytes.toString(CellUtil.cloneValue(cell)) + "--");
			cell = res.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("date"));
			System.out.print(Bytes.toString(CellUtil.cloneValue(cell)) + "--");
			cell = res.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("type"));
			System.out.print(Bytes.toString(CellUtil.cloneValue(cell)));
			System.out.println();
		}
	}

	@After
	public void destroy() {
		try {
			table.close();
			admin.close();
			connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
