package cn.crxy.spider.web.utils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.crxy.spider.web.domain.Article;


/**
 * solr工具类
 *
 */
public class SolrUtil {
	static final Logger logger = LoggerFactory.getLogger(SolrUtil.class);
	private static final String SOLR_URL = "http://192.168.1.170:8983/solr/collection1"; // 服务器地址
	private static HttpSolrServer server = null;
	static{
		try {
			server = new HttpSolrServer(SOLR_URL);
			server.setAllowCompression(true);
			server.setConnectionTimeout(10000);
			server.setDefaultMaxConnectionsPerHost(100);
			server.setMaxTotalConnections(100);
		} catch (Exception e) {
			logger.error("请检查tomcat服务器或端口是否开启!{}",e);
			e.printStackTrace();
		}
	}
	/**
	 * 建立索引
	 * @throws Exception
	 */
	public static void addIndex(Article article) {
		try {
			server.addBean(article);
			server.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 查询
	 * @param skey 
	 * @param row 
	 * @param start 
	 * @param sort 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static Map<String,Object> search(String skey, Long start, Long row) throws Exception {
		HashedMap map = new HashedMap();
		SolrQuery solrQuery = new SolrQuery();
		solrQuery.setQuery("title:"+skey+" describe:"+skey);
		solrQuery.setStart(start.intValue());
		solrQuery.setRows(row.intValue());
		//开启高亮
		solrQuery.setHighlight(true);
		solrQuery.addHighlightField("title");
		solrQuery.addHighlightField("describe");
		solrQuery.setHighlightSimplePre("<font color='red'>");
		solrQuery.setHighlightSimplePost("</font>");
		
		QueryResponse response = server.query(solrQuery);
		
		long numFound = response.getResults().getNumFound();
		List<Article> articles = response.getBeans(Article.class);
		Map<String, Map<String, List<String>>> highlighting = response.getHighlighting();
		
		for (Article article : articles) {
			Map<String, List<String>> map2 = highlighting.get(article.getId());
			List<String> titleList = map2.get("title");
			if(titleList!=null && titleList.size()>0){
				article.setTitle(titleList.get(0));
			}
			List<String> describeList = map2.get("describe");
			if(describeList!=null && describeList.size()>0){
				article.setDescribe(describeList.get(0));
			}
		}
		map.put("numFound", numFound);
		map.put("dataList", articles);
		return map;
	}
}