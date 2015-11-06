package org.apache.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.ContentStreamBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * This test demonstrated indexing json with a collection that uses a managed schema
 * and adds unknown fields on the fly. For all tests we stream one or more json documents
 * providing the split param and no field mappings in order to see what fields Solr creates.
 *
 * @author bbende
 */
public class IndexJSONSchemalessTest {

    static final String JSON_FLAT =
            "[{ " +
                "\"field1\" : \"doc1_field1\", " +
                "\"field2\" : \"doc1_field2\" " +
            "},{ " +
                "\"field1\" : \"doc2_field1\", " +
                "\"field2\" : \"doc2_field2\" " +
            "}]";

    static final String JSON_NESTING =
            "[{ " +
                "\"field1\" : \"doc1_field1\", " +
                "\"field2\" : [ " +
                    "{" +
                        "\"nested_field1\" : \"doc1_nested-doc1_field1\"," +
                        "\"nested_field2\" : \"doc1_nested-doc1_field2\"" +
                    "},{ " +
                        "\"nested_field1\" : \"doc1_nested-doc2_field1\"," +
                        "\"nested_field2\" : \"doc1_nested-doc2_field2\"" +
                    "}" +
                "] " +
            "},{ " +
                "\"field1\" : \"doc2_field1\", " +
                "\"field2\" : [ " +
                    "{" +
                        "\"nested_field1\" : \"doc2_nested-doc1_field1\"," +
                        "\"nested_field2\" : \"doc2_nested-doc1_field2\"" +
                    "},{ " +
                        "\"nested_field1\" : \"doc2_nested-doc2_field1\"," +
                        "\"nested_field2\" : \"doc2_nested-doc2_field2\"" +
                    "}" +
                "] " +
            "}]";

    private SolrClient solrClient;

    @Before
    public void setup() throws IOException {
        solrClient = EmbeddedSolrServerFactory.create("schemalessCollection");
    }

    @After
    public void teardown() throws IOException {
        if (solrClient != null) {
            solrClient.close();
        }
    }

    @Test
    public void testAddMultipleFlatJsonDocsSplitTopLevel()
            throws IOException, SolrServerException {

        ContentStreamUpdateRequest request = new ContentStreamUpdateRequest(
                "/update/json/docs");
        request.setParam("json.command", "false");
        request.setParam("split", "/");

        request.addContentStream(new ContentStreamBase.StringStream(JSON_FLAT));

        // should get two documents added
        QueryResponse qResponse = getQueryResponse(request);
        Assert.assertEquals(2, qResponse.getResults().getNumFound());

        for (SolrDocument solrDocument : qResponse.getResults()) {
            System.out.println(solrDocument);

            // each document should have field1 and field2, plus required fields of id and _version_
            Assert.assertEquals(4, solrDocument.getFieldNames().size());
            Assert.assertTrue(solrDocument.containsKey("id"));
            Assert.assertTrue(solrDocument.containsKey("_version_"));
            Assert.assertTrue(solrDocument.containsKey("field1"));
            Assert.assertTrue(solrDocument.containsKey("field2"));
        }
    }

    @Test
    public void testAddMultipleJsonDocsWithNestingSplitTopLevel()
            throws IOException, SolrServerException {

        ContentStreamUpdateRequest request = new ContentStreamUpdateRequest(
                "/update/json/docs");
        request.setParam("json.command", "false");
        request.setParam("split", "/");

        request.addContentStream(new ContentStreamBase.StringStream(JSON_NESTING));

        // should get 2 documents back because we split on /
        QueryResponse qResponse = getQueryResponse(request);
        Assert.assertEquals(2, qResponse.getResults().getNumFound());

        for (SolrDocument solrDocument : qResponse.getResults()) {
            System.out.println(solrDocument);

            // each document should have field1, field2.nested_field1, and field2.nested_field2
            // plus required fields of id and _version_
            Assert.assertEquals(5, solrDocument.getFieldNames().size());
            Assert.assertTrue(solrDocument.containsKey("id"));
            Assert.assertTrue(solrDocument.containsKey("_version_"));
            Assert.assertTrue(solrDocument.containsKey("field1"));
            Assert.assertTrue(solrDocument.containsKey("field2.nested_field1"));
            Assert.assertTrue(solrDocument.containsKey("field2.nested_field2"));

            // field2.nested_field1 should have 2 values since we split at the top-level
            Assert.assertEquals(2, solrDocument.getFieldValues("field2.nested_field1").size());

            // field2.nested_field2 should have 2 values since we split at the top-level
            Assert.assertEquals(2, solrDocument.getFieldValues("field2.nested_field2").size());
        }
    }

    @Test
    public void testAddMultipleJsonDocsWithNestingSplitOnNestedDocs()
            throws IOException, SolrServerException {

        ContentStreamUpdateRequest request = new ContentStreamUpdateRequest(
                "/update/json/docs");
        request.setParam("json.command", "false");
        request.setParam("split", "/field2");

        request.addContentStream(new ContentStreamBase.StringStream(JSON_NESTING));

        // should get 4 documents back because we split on field2
        QueryResponse qResponse = getQueryResponse(request);
        Assert.assertEquals(4, qResponse.getResults().getNumFound());

        for (SolrDocument solrDocument : qResponse.getResults()) {
            System.out.println(solrDocument);

            // each document should have field1, field2.nested_field1, and field2.nested_field2
            // plus required fields of id and _version_
            Assert.assertEquals(5, solrDocument.getFieldNames().size());
            Assert.assertTrue(solrDocument.containsKey("id"));
            Assert.assertTrue(solrDocument.containsKey("_version_"));
            Assert.assertTrue(solrDocument.containsKey("field1"));
            Assert.assertTrue(solrDocument.containsKey("field2.nested_field1"));
            Assert.assertTrue(solrDocument.containsKey("field2.nested_field2"));

            // field2.nested_field1 should have 1 value since we split on field2
            Assert.assertEquals(1, solrDocument.getFieldValues("field2.nested_field1").size());

            // field2.nested_field2 should have 1 value since we split on field2
            Assert.assertEquals(1, solrDocument.getFieldValues("field2.nested_field2").size());
        }
    }

    private QueryResponse getQueryResponse(AbstractUpdateRequest request)
            throws org.apache.solr.client.solrj.SolrServerException, IOException {
        UpdateResponse response = request.process(solrClient);
        Assert.assertEquals(0, response.getStatus());

        solrClient.commit();

        // verify number of results
        SolrQuery query = new SolrQuery("*:*");
        QueryResponse qResponse = solrClient.query(query);
        return qResponse;
    }
}
