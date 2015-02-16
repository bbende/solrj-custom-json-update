package org.apache.solr;

import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;

/**
 * A convenience class for streaming JSON to Apache Solr's custom JSON Update Handler.
 *
 * @author bbende
 */
public class JSONUpdateRequest extends AbstractUpdateRequest {

    private final InputStream jsonInputStream;

    /**
     * Construct a new update request for the given InputStream.
     *
     * @param jsonInputStream
     */
    public JSONUpdateRequest(InputStream jsonInputStream) {
        super(METHOD.POST, "/update/json/docs");
        this.jsonInputStream = jsonInputStream;
        this.setParam("json.command", "false");
    }

    /**
     * Adds a field mapping which results in "f=field:jsonPath".
     *
     * @param field
     * @param jsonPath
     */
    public void addFieldMapping(String field, String jsonPath) {
        getParams().add("f", field + ":" + jsonPath);
    }

    /**
     * Sets the split param.
     *
     * @param jsonPath
     */
    public void setSplit(String jsonPath) {
        setParam("split", jsonPath);
    }

    @Override
    public Collection<ContentStream> getContentStreams() throws IOException {
        ContentStream jsonContentStream = new InputStreamContentStream(
                jsonInputStream, "application/json");
        return Collections.singletonList(jsonContentStream);
    }

    /**
     * A ContentStream for wrapping an InputStream.
     */
    private class InputStreamContentStream extends ContentStreamBase {

        private final InputStream inputStream;

        public InputStreamContentStream(InputStream inputStream, String contentType) {
            this.inputStream = inputStream;
            this.setContentType(contentType);
        }

        @Override
        public InputStream getStream() throws IOException {
            return inputStream;
        }
    }

}
