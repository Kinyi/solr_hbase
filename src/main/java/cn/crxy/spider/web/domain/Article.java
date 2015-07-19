package cn.crxy.spider.web.domain;

import org.apache.solr.client.solrj.beans.Field;

/**
 * 文章实体类
 * @author Administrator
 *
 */
public class Article {
	
	@Field
	private String id;
	@Field
	private String title;
	
	@Field
	private String describe;
	
	private String content;
	@Field
	private String author;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public String getAuthor() {
		return author;
	}
	public void setAuthor(String author) {
		this.author = author;
	}
	public String getDescribe() {
		return describe;
	}
	public void setDescribe(String describe) {
		this.describe = describe;
	}
}
