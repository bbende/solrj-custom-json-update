package org.apache.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * @author bbende
 */
public class IndexJSONTest {

    // Example JSON to use as input
    static final String EXAMPLE_JSON = "{\n" +
            "  \"first\": \"John\",\n" +
            "  \"last\": \"Doe\",\n" +
            "  \"grade\": 8,\n" +
            "  \"exams\": [\n" +
            "      {\n" +
            "        \"subject\": \"Maths\",\n" +
            "        \"test\"   : \"term1\",\n" +
            "        \"marks\":90},\n" +
            "        {\n" +
            "         \"subject\": \"Biology\",\n" +
            "         \"test\"   : \"term1\",\n" +
            "         \"marks\":86}\n" +
            "      ]\n" +
            "}";

    // Example JSON to use as input
    static final String EXAMPLE_JSON2 = "{\n" +
            "  \"first\": \"Bob\",\n" +
            "  \"last\": \"Smith\",\n" +
            "  \"grade\": 8,\n" +
            "  \"exams\": [\n" +
            "      {\n" +
            "        \"subject\": \"Maths\",\n" +
            "        \"test\"   : \"term1\",\n" +
            "        \"marks\":91},\n" +
            "        {\n" +
            "         \"subject\": \"Biology\",\n" +
            "         \"test\"   : \"term1\",\n" +
            "         \"marks\":87}\n" +
            "      ]\n" +
            "}";


    // Expected SolrDocuments to be produced from JSON
    static final Collection<SolrDocument> EXPECTED_SOLR_DOCS;

    static {
        SolrDocument doc1 = new SolrDocument();
        doc1.addField("first", "John");
        doc1.addField("last", "Doe");
        doc1.addField("grade", 8);
        doc1.addField("subject", "Maths");
        doc1.addField("test", "term1");
        doc1.addField("marks", 90);

        SolrDocument doc2 = new SolrDocument();
        doc2.addField("first", "John");
        doc2.addField("last", "Doe");
        doc2.addField("grade", 8);
        doc2.addField("subject", "Biology");
        doc2.addField("test", "term1");
        doc2.addField("marks", 86);

        Collection<SolrDocument> solrDocuments = new ArrayList<>();
        solrDocuments.add(doc1);
        solrDocuments.add(doc2);
        EXPECTED_SOLR_DOCS = Collections.unmodifiableCollection(solrDocuments);
    }

    // Expected SolrDocuments to be produced from JSON
    static final Collection<SolrDocument> EXPECTED_SOLR_DOCS2;

    static {
        SolrDocument doc1 = new SolrDocument();
        doc1.addField("first", "Bob");
        doc1.addField("last", "Smith");
        doc1.addField("grade", 8);
        doc1.addField("subject", "Maths");
        doc1.addField("test", "term1");
        doc1.addField("marks", 90);

        SolrDocument doc2 = new SolrDocument();
        doc2.addField("first", "Bob");
        doc2.addField("last", "Smith");
        doc2.addField("grade", 8);
        doc2.addField("subject", "Biology");
        doc2.addField("test", "term1");
        doc2.addField("marks", 86);

        Collection<SolrDocument> solrDocuments = new ArrayList<>();
        solrDocuments.add(doc1);
        solrDocuments.add(doc2);
        EXPECTED_SOLR_DOCS2 = Collections.unmodifiableCollection(solrDocuments);
    }

    private SolrClient solrClient;

    @Before
    public void setup() throws IOException {
        solrClient = EmbeddedSolrServerFactory.create("jsonCollection");
    }

    @After
    public void teardown() throws IOException {
        if (solrClient != null) {
            solrClient.close();
        }
    }

    @Test
    public void testAddCustomJsonWithJSONUpdateRequest() throws IOException {
        byte[] jsonBytes = EXAMPLE_JSON.getBytes("UTF-8");
        InputStream jsonInput = new ByteArrayInputStream(jsonBytes);

        JSONUpdateRequest request = new JSONUpdateRequest(jsonInput);
        request.setSplit("/exams");
        request.addFieldMapping("first", "/first");
        request.addFieldMapping("last", "/last");
        request.addFieldMapping("grade", "/grade");
        request.addFieldMapping("subject", "/exams/subject");
        request.addFieldMapping("test", "/exams/test");
        request.addFieldMapping("marks", "/exams/marks");

        runUpdateRequestTest(request, EXPECTED_SOLR_DOCS);
    }

    @Test
    public void testAddCustomJsonWithContentStreamUpdateRequest() throws IOException {
        ContentStreamUpdateRequest request = new ContentStreamUpdateRequest(
                "/update/json/docs");
        request.setParam("json.command", "false");
        request.setParam("split", "/exams");
        request.getParams().add("f", "first:/first");
        request.getParams().add("f", "last:/last");
        request.getParams().add("f", "grade:/grade");
        request.getParams().add("f", "subject:/exams/subject");
        request.getParams().add("f", "test:/exams/test");
        request.getParams().add("f", "marks:/exams/marks");

        request.addContentStream(new ContentStreamBase.StringStream(EXAMPLE_JSON));

        runUpdateRequestTest(request, EXPECTED_SOLR_DOCS);
    }

    @Test
    public void testAddMultipleContentStreamsWithContentStreamUpdateRequest() throws IOException {
        ContentStreamUpdateRequest request = new ContentStreamUpdateRequest(
                "/update/json/docs");
        request.setParam("json.command", "false");
        request.setParam("split", "/exams");
        request.getParams().add("f", "first:/first");
        request.getParams().add("f", "last:/last");
        request.getParams().add("f", "grade:/grade");
        request.getParams().add("f", "subject:/exams/subject");
        request.getParams().add("f", "test:/exams/test");
        request.getParams().add("f", "marks:/exams/marks");

        // add two streams...
        request.addContentStream(new ContentStreamBase.StringStream(EXAMPLE_JSON));
        request.addContentStream(new ContentStreamBase.StringStream(EXAMPLE_JSON2));

        // ensure docs from both streams were added
        Collection<SolrDocument> solrDocuments = new ArrayList<>();
        solrDocuments.addAll(EXPECTED_SOLR_DOCS);
        solrDocuments.addAll(EXPECTED_SOLR_DOCS2);

        runUpdateRequestTest(request, solrDocuments);
    }

    @Test
    public void testAddMultipleJsonDocsWithContentStreamUpdateRequest() throws IOException {
        ContentStreamUpdateRequest request = new ContentStreamUpdateRequest(
                "/update/json/docs");
        request.setParam("json.command", "false");
        request.setParam("split", "/exams");
        request.getParams().add("f", "first:/first");
        request.getParams().add("f", "last:/last");
        request.getParams().add("f", "grade:/grade");
        request.getParams().add("f", "subject:/exams/subject");
        request.getParams().add("f", "test:/exams/test");
        request.getParams().add("f", "marks:/exams/marks");

        // add one stream with two documents...
        final String jsonWithTwoDocs = "[" + EXAMPLE_JSON + "," + EXAMPLE_JSON2 + "]";
        //final String jsonWithTwoDocs = EXAMPLE_JSON + EXAMPLE_JSON2;
        request.addContentStream(new ContentStreamBase.StringStream(jsonWithTwoDocs));

        // ensure both docs were added
        Collection<SolrDocument> solrDocuments = new ArrayList<>();
        solrDocuments.addAll(EXPECTED_SOLR_DOCS);
        solrDocuments.addAll(EXPECTED_SOLR_DOCS2);

        runUpdateRequestTest(request, solrDocuments);
    }


    private void runUpdateRequestTest(AbstractUpdateRequest request,
            Collection<SolrDocument> expectedSolrDocuments) {
        try {
            UpdateResponse response = request.process(solrClient);
            Assert.assertEquals(0, response.getStatus());

            solrClient.commit();

            // verify number of results
            SolrQuery query = new SolrQuery("*:*");
            QueryResponse qResponse = solrClient.query(query);
            Assert.assertEquals(expectedSolrDocuments.size(), qResponse.getResults().getNumFound());

            // verify documents have expected fields and values
            for (SolrDocument expectedDoc : expectedSolrDocuments) {
                boolean found = false;
                for (SolrDocument solrDocument : qResponse.getResults()) {
                    boolean foundAllFields = true;
                    for (String expectedField : expectedDoc.getFieldNames()) {
                        Object expectedVal = expectedDoc.getFirstValue(expectedField);
                        Object actualVal = solrDocument.getFirstValue(expectedField);
                        foundAllFields = expectedVal.equals(actualVal);
                    }

                    if (foundAllFields) {
                        found = true;
                        break;
                    }
                }
                Assert.assertTrue(found);
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Encountered exception: " + e.getMessage());
        } finally {
            try {
                solrClient.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
