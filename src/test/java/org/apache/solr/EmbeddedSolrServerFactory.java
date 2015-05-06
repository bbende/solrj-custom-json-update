package org.apache.solr;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Helper to create EmbeddedSolrServer instances for testing.
 *
 * @author bryanbende
 */
public class EmbeddedSolrServerFactory {

    public static final String DEFAULT_SOLR_HOME = "src/test/resources/solr";

    public static final String DEFAULT_CORE_HOME = "src/test/resources/";

    public static final String DEFAULT_DATA_DIR = "/tmp";

    /**
     * Use the defaults to create the core.
     *
     * @param coreName
     * @return
     */
    public static SolrClient create(String coreName) throws IOException {
        return create(DEFAULT_SOLR_HOME, DEFAULT_CORE_HOME,
                coreName, DEFAULT_DATA_DIR);
    }

    /**
     *
     * @param solrHome
     *              path to directory where solr.xml lives
     *
     * @param coreName
     *              the name of the core to load
     * @param dataDir
     *              the data dir for the core
     *
     * @return an EmbeddedSolrServer for the given core
     */
    public static SolrClient create(String solrHome, String coreHome, String coreName, String dataDir) throws IOException {
        File coreDataDir = new File(dataDir + "/" + coreName);
        if (coreDataDir.exists()) {
           FileUtils.deleteDirectory(coreDataDir);
        }

        CoreContainer coreContainer = new CoreContainer(solrHome);
        coreContainer.load();

        Properties props = new Properties();
        props.setProperty("dataDir", dataDir + "/" + coreName);

        CoreDescriptor descriptor = new CoreDescriptor(coreContainer, coreName,
                new File(coreHome, coreName).getAbsolutePath(), props);

        SolrCore solrCore = coreContainer.create(descriptor);
        //coreContainer.register(solrCore, false);
        return new EmbeddedSolrServer(coreContainer, coreName);
    }
}
