package com.alibaba.otter.canal.client.adapter.es.test;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ESTest {

    private RestHighLevelClient restHighLevelClient;

    @Before
    public void init() throws UnknownHostException {
        Settings.Builder settingBuilder = Settings.builder();
        settingBuilder.put("cluster.name", TestConstant.clusterName);
        List<HttpHost> httpHostList = new ArrayList<>();
        String[] hostArray = TestConstant.esHosts.split(",");
        for (String host : hostArray) {
            int i = host.indexOf(":");
            httpHostList.add(new HttpHost(InetAddress.getByName(host.substring(0, i)), Integer.parseInt(host
                    .substring(i + 1))));

        }
        restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(httpHostList.toArray(new HttpHost[httpHostList.size()])));
    }

    @Test
    public void test01() throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(
                QueryBuilders.termQuery("_id", "1")).size(10000);
        SearchRequest searchRequest = new SearchRequest(
                "test").types("osm").source(searchSourceBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsMap().get("data").getClass());
        }
    }

    @Test
    public void test02() throws IOException {
        Map<String, Object> esFieldData = new LinkedHashMap<>();
        esFieldData.put("userId", 2L);
        esFieldData.put("eventId", 4L);
        esFieldData.put("eventName", "网络异常");
        esFieldData.put("description", "第四个事件信息");

        Map<String, Object> relations = new LinkedHashMap<>();
        esFieldData.put("user_event", relations);
        relations.put("name", "event");
        relations.put("parent", "2");

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(
                "test", "osm", "2_4"
        ).routing("2").source(esFieldData));

        commit(bulkRequest);
    }

    @Test
    public void test03() throws IOException {
        Map<String, Object> esFieldData = new LinkedHashMap<>();
        esFieldData.put("userId", 2L);
        esFieldData.put("eventName", "网络异常1");

        Map<String, Object> relations = new LinkedHashMap<>();
        esFieldData.put("user_event", relations);
        relations.put("name", "event");
        relations.put("parent", "2");

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new UpdateRequest(
                "test", "osm", "2_4"
        ).routing("2").doc(esFieldData));
        commit(bulkRequest);
    }

    @Test
    public void test04() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new DeleteRequest(
                "test", "osm", "2_4"
        ));
        commit(bulkRequest);
    }

    private void commit(BulkRequest bulkRequest) throws IOException {
        if (bulkRequest.numberOfActions() > 0) {
            BulkResponse response = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            if (response.hasFailures()) {
                for (BulkItemResponse itemResponse : response.getItems()) {
                    if (!itemResponse.isFailed()) {
                        continue;
                    }

                    if (itemResponse.getFailure().getStatus() == RestStatus.NOT_FOUND) {
                        System.out.println(itemResponse.getFailureMessage());
                    } else {
                        System.out.println("ES bulk commit error" + itemResponse.getFailureMessage());
                    }
                }
            }
        }
    }

    @After
    public void after() throws IOException {
        restHighLevelClient.close();
    }
}
