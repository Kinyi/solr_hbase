package cn.crxy.spider.web.dataimport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import cn.crxy.spider.web.domain.Article;
import cn.crxy.spider.web.utils.HbaseUtils;
import cn.crxy.spider.web.utils.SolrUtil;
/**
 * 模拟数据源往solr、和hbase中导入数据
 * @author Administrator
 *
 */
public class DataImportAndIndex {
	
	public static void main(String[] args) throws java.lang.Exception {
		List<Article> arrayList = new ArrayList<Article>();
		//TODO 从其他数据源读取数据
		File file = new File("D:\\solr课程\\2015-04-29\\article.txt");
		if(!file.exists()){
			System.err.println("文件不存在！");
		}
		InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file), "UTF-8");
		
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		String lineString = null;
		while((lineString = bufferedReader.readLine())!=null){
			Article article = new Article();
			String[] split = lineString.split("\t");
			article.setId(split[0]);
			article.setTitle(split[1]);
			article.setAuthor(split[2]);
			article.setDescribe(split[3]);
			article.setContent(split[3]);
			arrayList.add(article);
		}
		
		HbaseUtils hbaseUtils = new HbaseUtils();
		for (Article article : arrayList) {
			//把数据插入solr
			SolrUtil.addIndex(article);
			//把数据插入hbase
			try {
				hbaseUtils.put(hbaseUtils.TABLE_NAME, article.getId()+"", hbaseUtils.COLUMNFAMILY_1, hbaseUtils.COLUMNFAMILY_1_TITLE, article.getTitle());
				hbaseUtils.put(hbaseUtils.TABLE_NAME, article.getId()+"", hbaseUtils.COLUMNFAMILY_1, hbaseUtils.COLUMNFAMILY_1_AUTHOR, article.getAuthor());
				hbaseUtils.put(hbaseUtils.TABLE_NAME, article.getId()+"", hbaseUtils.COLUMNFAMILY_1, hbaseUtils.COLUMNFAMILY_1_DESCRIBE, article.getDescribe());
				hbaseUtils.put(hbaseUtils.TABLE_NAME, article.getId()+"", hbaseUtils.COLUMNFAMILY_1, hbaseUtils.COLUMNFAMILY_1_CONTENT, article.getContent());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		inputStreamReader.close();
		bufferedReader.close();
	}
}