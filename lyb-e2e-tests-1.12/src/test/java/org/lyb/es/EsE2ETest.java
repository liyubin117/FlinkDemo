package org.lyb.es;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

public class EsE2ETest {
    private ElasticsearchContainer elasticsearchContainer;

    @Before
    public void before() {
        elasticsearchContainer =
                new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.9.2");
        elasticsearchContainer.start();
    }

    @After
    public void after() {
        elasticsearchContainer.stop();
    }

    @Test
    public void test1() {}
}
