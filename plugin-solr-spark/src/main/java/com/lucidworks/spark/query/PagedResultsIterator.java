package com.lucidworks.spark.query;

import com.lucidworks.spark.SolrRDD;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Base class for iterating over paged results in a Solr QueryResponse, with the
 * most obvious example being iterating over SolrDocument objects matching a query.
 */
public abstract class PagedResultsIterator<T> implements Iterator<T>, Iterable<T> {

  protected static final int DEFAULT_PAGE_SIZE = 50;

  protected SolrServer solrServer;
  protected SolrQuery solrQuery;
  protected int currentPageSize = 0;
  protected int iterPos = 0;
  protected long totalDocs = 0;
  protected long numDocs = 0;
  protected String cursorMark = null;
  protected boolean closeAfterIterating = false;

  protected List<T> currentPage;

  public PagedResultsIterator(SolrServer solrServer, SolrQuery solrQuery) {
    this(solrServer, solrQuery, null);
  }

  public PagedResultsIterator(SolrServer solrServer, SolrQuery solrQuery, String cursorMark) {
    this.solrServer = solrServer;
    this.closeAfterIterating = !(solrServer instanceof CloudSolrServer);
    this.solrQuery = solrQuery;
    this.cursorMark = cursorMark;
    if (solrQuery.getRows() == null)
      solrQuery.setRows(DEFAULT_PAGE_SIZE); // default page size
  }

  public boolean hasNext() {
    if (currentPage == null || iterPos == currentPageSize) {
      try {
        currentPage = fetchNextPage();
        currentPageSize = currentPage.size();
        iterPos = 0;
      } catch (SolrServerException sse) {
        throw new RuntimeException(sse);
      }
    }
    boolean hasNext = (iterPos < currentPageSize);
    if (!hasNext && closeAfterIterating) {
      try {
        solrServer.shutdown();
      } catch (Exception exc) {}
    }
    return hasNext;
  }

  protected int getStartForNextPage() {
    Integer currentStart = solrQuery.getStart();
    return (currentStart != null) ? currentStart + solrQuery.getRows() : 0;
  }

  protected List<T> fetchNextPage() throws SolrServerException {
    int start = (cursorMark != null) ? 0 : getStartForNextPage();
    QueryResponse resp = SolrRDD.querySolr(solrServer, solrQuery, start, cursorMark);
    if (cursorMark != null)
      cursorMark = resp.getNextCursorMark();

    iterPos = 0;
    SolrDocumentList docs = resp.getResults();
    totalDocs = docs.getNumFound();
    return processQueryResponse(resp);
  }

  protected abstract List<T> processQueryResponse(QueryResponse resp);

  public T next() {
    if (currentPage == null || iterPos >= currentPageSize)
      throw new NoSuchElementException("No more docs available!");

    ++numDocs;

    return currentPage.get(iterPos++);
  }

  public void remove() {
    throw new UnsupportedOperationException("remove is not supported");
  }

  public Iterator<T> iterator() {
    return this;
  }
}