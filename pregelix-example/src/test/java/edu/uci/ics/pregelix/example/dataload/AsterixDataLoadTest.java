/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.pregelix.example.dataload;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.Test;

import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.core.jobgen.JobGenOuterJoin;
import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.pregelix.core.optimizer.IOptimizer;
import edu.uci.ics.pregelix.core.optimizer.NoOpOptimizer;
import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;
import edu.uci.ics.pregelix.example.ConnectedComponentsVertex;
import edu.uci.ics.pregelix.example.ConnectedComponentsVertex.SimpleConnectedComponentsVertexOutputFormat;
import edu.uci.ics.pregelix.example.inputformat.TextConnectedComponentsInputFormat;
import edu.uci.ics.pregelix.example.util.TestUtils;

public class AsterixDataLoadTest {
    private static final String EXPECT_RESULT_DIR = "src/test/resources/asterixLoading/";
    private static final String ACTUAL_RESULT_DIR = "actual";
    private static final String NC1 = "nc1";

    private static final Logger LOGGER = Logger.getLogger(AsterixDataLoadTest.class.getName());

    private static final String PATH_TO_CLUSTER_STORE = "src/test/resources/cluster/stores.properties";
    private static final String PATH_TO_CLUSTER_STORE_SINGLE = "src/test/resources/cluster/singlestore.properties";
    private static final String PATH_TO_CLUSTER_PROPERTIES = "src/test/resources/cluster/cluster.properties";

    private static final String HYRACKS_APP_NAME = "asterixLoading";
    private static final String JOB_NAME = "AsterixDataLoadTest";

    private static final String ASTERIX_INPUT_INDEX_PATHS = "asterix://nc1"
            + (new File("src/test/resources/asterixLoading/nc1data/Pregelix/Nodes_idx_Nodes/").getAbsolutePath())
            + ",asterix://nc2"
            + (new File("src/test/resources/asterixLoading/nc2data/Pregelix/Nodes_idx_Nodes/").getAbsolutePath());

    private JobGenOuterJoin giraphTestJobGen;
    private PregelixJob job;

    public AsterixDataLoadTest() throws Exception {
        job = new PregelixJob(JOB_NAME);
        job.setVertexClass(ConnectedComponentsVertex.class);
        job.setVertexInputFormatClass(TextConnectedComponentsInputFormat.class);
        job.getConfiguration().setClass(PregelixJob.VERTEX_INDEX_CLASS, LongWritable.class, WritableComparable.class);
        job.getConfiguration().setClass(PregelixJob.VERTEX_VALUE_CLASS, LongWritable.class, Writable.class);
        job.setVertexOutputFormatClass(SimpleConnectedComponentsVertexOutputFormat.class);
        job.getConfiguration().setClass(PregelixJob.EDGE_VALUE_CLASS, LongWritable.class, Writable.class);
        job.getConfiguration().setClass(PregelixJob.MESSAGE_VALUE_CLASS, LongWritable.class, Writable.class);
        job.getConfiguration().set(PregelixJob.JOB_ID, "test_job");
        FileInputFormat.setInputPaths(job, ASTERIX_INPUT_INDEX_PATHS);
    }

    public void setUp(String storePath) throws Exception {
        ClusterConfig.setStorePath(storePath);
        ClusterConfig.setClusterPropertiesPath(PATH_TO_CLUSTER_PROPERTIES);
        ClusterConfig.clearConnection();
        cleanupStores();
        PregelixHyracksIntegrationUtil.init();
        LOGGER.info("Hyracks mini-cluster started");
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));

        IOptimizer dynamicOptimizer = new NoOpOptimizer();
        giraphTestJobGen = new JobGenOuterJoin(job, dynamicOptimizer);
    }

    private void cleanupStores() throws IOException {
        FileUtils.forceMkdir(new File("teststore"));
        FileUtils.forceMkdir(new File("build"));
        FileUtils.cleanDirectory(new File("teststore"));
        FileUtils.cleanDirectory(new File("build"));
    }

    public void tearDown() throws Exception {
        PregelixHyracksIntegrationUtil.deinit();
        LOGGER.info("Hyracks mini-cluster shut down");
    }

    @Test
    public void testSamePartitioning() throws Exception {
        setUp(PATH_TO_CLUSTER_STORE_SINGLE);
        runCreation();
        runDataLoad();
        runIndexScan();
        try {
            compareResults();
        } catch (Exception e) {
            tearDown();
            throw e;
        }
        tearDown();
    }

    @Test
    public void testDifferentPartitioning() throws Exception {
        setUp(PATH_TO_CLUSTER_STORE);
        runCreation();
        runDataLoad();
        runIndexScan();
        try {
            compareResults();
        } catch (Exception e) {
            tearDown();
            throw e;
        }
        tearDown();
    }

    private void runCreation() throws Exception {
        try {
            JobSpecification bulkLoadJobSpec = giraphTestJobGen.generateCreatingJob();
            PregelixHyracksIntegrationUtil.runJob(bulkLoadJobSpec, HYRACKS_APP_NAME);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runDataLoad() throws Exception {
        try {
            JobSpecification bulkLoadJobSpec = giraphTestJobGen.generateLoadingJob();
            PregelixHyracksIntegrationUtil.runJob(bulkLoadJobSpec, HYRACKS_APP_NAME);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runIndexScan() throws Exception {
        try {
            JobSpecification scanSortPrintJobSpec = giraphTestJobGen.scanIndexPrintGraph(NC1, ACTUAL_RESULT_DIR
                    + File.separator + job.getJobName());
            PregelixHyracksIntegrationUtil.runJob(scanSortPrintJobSpec, HYRACKS_APP_NAME);
        } catch (Exception e) {
            throw e;
        }
    }

    private void compareResults() throws Exception {
        PregelixJob job = new PregelixJob(JOB_NAME);
        TestUtils.compareWithResult(new File(EXPECT_RESULT_DIR + File.separator + job.getJobName()), new File(
                ACTUAL_RESULT_DIR + File.separator + job.getJobName()));
    }
}
