package com.github.xtermi2.elasticsearch;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.client.RequestOptions.DEFAULT;

@Testcontainers
public class RHLCMsearchScrollIdTest {

    private static final String ES_VERSION = System.getProperty("es.version", "7.6.1");
    @Container
    ElasticsearchContainer elasticsearch = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + ES_VERSION);

    private RestHighLevelClient restHighLevelClient;
    private Client transportClient;
    private static final int NUMBER_OF_DOCUMENTS = 110;
    private static final int SCROLL_PAGE_SIZE = 100;

    @BeforeEach
    void setUp() throws IOException {
        // setup RestHighLevelClient
        restHighLevelClient = new RestHighLevelClient(RestClient.builder(
                new HttpHost("localhost", elasticsearch.getMappedPort(9200), "http")));
        MainResponse info = restHighLevelClient.info(DEFAULT);
        assertThat(info.getVersion().getNumber())
                .as("ES version")
                .isEqualTo(ES_VERSION);

        // setup TransportClient
        transportClient = new PreBuiltTransportClient(Settings.builder()
                .put("cluster.name", info.getClusterName())
                .build())
                .addTransportAddress(new TransportAddress(elasticsearch.getTcpHost()));
        assertThat(transportClient.admin().cluster().prepareHealth()
                .setWaitForGreenStatus()
                .get()
                .getStatus())
                .as("ES cluster health status")
                .isEqualTo(ClusterHealthStatus.GREEN);
    }


    @Test
    void restHighLevelClient_search_should_return_scrollId() throws IOException {
        String indexName = generateDummyDocumentsInNewRandomIndex(NUMBER_OF_DOCUMENTS);
        SearchRequest searchRequest = createSearchRequest(SCROLL_PAGE_SIZE, indexName);

        SearchResponse res = restHighLevelClient.search(searchRequest, DEFAULT);

        assertThat(res.getHits().getTotalHits().value)
                .as("totalHits")
                .isEqualTo((long) NUMBER_OF_DOCUMENTS);
        assertThat(res.getHits().getHits().length)
                .as("getHits().length")
                .isEqualTo(SCROLL_PAGE_SIZE);
        if (res.getHits().getTotalHits().value > res.getHits().getHits().length) {
            assertThat(res.getScrollId())
                    .as("ScrollId")
                    .isNotBlank();
        } else {
            assertThat(res.getScrollId())
                    .as("ScrollId")
                    .isNull();
        }
    }

    @Test
    void restHighLevelClient_msearch_should_return_scrollId() throws IOException {
        MultiSearchRequest multiSearchRequest = createMSearchRequest();

        MultiSearchResponse msearchRes = restHighLevelClient.msearch(multiSearchRequest, DEFAULT);

        validateMsearchResponse(msearchRes);
    }

    @Test
    void transportClient_msearch_should_return_scrollId() throws IOException {
        MultiSearchRequest multiSearchRequest = createMSearchRequest();

        MultiSearchResponse msearchRes = transportClient.multiSearch(multiSearchRequest).actionGet();

        validateMsearchResponse(msearchRes);
    }

    private void validateMsearchResponse(MultiSearchResponse msearchRes) {
        for (int i = 0; i < msearchRes.getResponses().length; i++) {
            MultiSearchResponse.Item item = msearchRes.getResponses()[i];
            assertThat(item.isFailure())
                    .as("msearch[%s].hasFailures: %s", i, item.getFailureMessage())
                    .isEqualTo(false);
            SearchResponse res = item.getResponse();
            assertThat(res.getHits().getTotalHits().value)
                    .as("msearch[%s].totalHits", i)
                    .isEqualTo((long) NUMBER_OF_DOCUMENTS);
            assertThat(res.getHits().getHits().length)
                    .as("msearch[%s].getHits().length", i)
                    .isEqualTo(SCROLL_PAGE_SIZE);
            if (res.getHits().getTotalHits().value > res.getHits().getHits().length) {
                assertThat(res.getScrollId())
                        .as("msearch[%s].ScrollId", i)
                        .isNotBlank();
            } else {
                assertThat(res.getScrollId())
                        .as("ScrollId")
                        .isNull();
            }
        }
    }

    private MultiSearchRequest createMSearchRequest() throws IOException {
        String indexName = generateDummyDocumentsInNewRandomIndex(NUMBER_OF_DOCUMENTS);
        SearchRequest searchRequest = createSearchRequest(SCROLL_PAGE_SIZE, indexName);
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        multiSearchRequest.add(searchRequest);
        return multiSearchRequest;
    }

    private SearchRequest createSearchRequest(int size, String indexName) {
        SearchRequest searchRequest = new SearchRequest(indexName)
                .scroll(TimeValue.timeValueSeconds(30));
        searchRequest.source()
                .query(QueryBuilders.matchAllQuery())
                .size(size);
        return searchRequest;
    }

    private String generateDummyDocumentsInNewRandomIndex(int numberOfDocuments) throws IOException {
        String indexName = RandomStringUtils.randomAlphabetic(15).toLowerCase();
        BulkRequest bulkRequest = new BulkRequest()
                .setRefreshPolicy(IMMEDIATE);
        IntStream.range(0, numberOfDocuments)
                .forEach(i -> bulkRequest.add(new IndexRequest(indexName)
                        .id(String.valueOf(i))
                        .source(ImmutableMap.of(
                                "documentNumber", i,
                                "randomData", RandomStringUtils.random(20)
                                ),
                                XContentType.JSON)));
        BulkResponse res = restHighLevelClient.bulk(bulkRequest, DEFAULT);
        assertThat(res.hasFailures())
                .as("generateDummyDocumentsInNewRandomIndex: bulk.hasFailures: %s", res.buildFailureMessage())
                .isFalse();
        return indexName;
    }
}
