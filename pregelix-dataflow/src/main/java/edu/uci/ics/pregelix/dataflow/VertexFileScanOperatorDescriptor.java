/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.pregelix.dataflow;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexInputFormat;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;

@SuppressWarnings("rawtypes")
public class VertexFileScanOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final Logger LOGGER = Logger.getLogger(VertexFileScanOperatorDescriptor.class.getName());
    private static final long serialVersionUID = 1L;
    private final List<InputSplit> splits;
    private final IConfigurationFactory confFactory;
    private final int fieldSize = 2;

    /**
     * @param spec
     */
    public VertexFileScanOperatorDescriptor(JobSpecification spec, RecordDescriptor rd, List<InputSplit> splits,
            IConfigurationFactory confFactory) throws HyracksException {
        super(spec, 0, 1);
        this.splits = splits;
        this.confFactory = confFactory;
        this.recordDescriptors[0] = rd;
    }

    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
            throws HyracksDataException {
        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            private Configuration conf = confFactory.createConfiguration();

            @Override
            public void initialize() throws HyracksDataException {
                try {
                    Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
                    writer.open();
                    loadVertices(ctx, partition);
                    writer.close();
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }

            /**
             * Load the vertices
             * 
             * @parameter IHyracks ctx
             * 
             * @throws IOException
             * @throws IllegalAccessException
             * @throws InstantiationException
             * @throws ClassNotFoundException
             * @throws InterruptedException
             */
            @SuppressWarnings("unchecked")
            private void loadVertices(final IHyracksTaskContext ctx, int partitionId) throws IOException,
                    ClassNotFoundException, InterruptedException, InstantiationException, IllegalAccessException {
                ByteBuffer frame = ctx.allocateFrame();
                FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                appender.reset(frame, true);

                VertexInputFormat vertexInputFormat = BspUtils.createVertexInputFormat(conf);
                TaskAttemptContext context = new TaskAttemptContext(conf, new TaskAttemptID());
                InputSplit split = splits.get(partition);

                if (split instanceof FileSplit) {
                    FileSplit fileSplit = (FileSplit) split;
                    LOGGER.info("read file split: " + fileSplit.getPath() + " location:" + fileSplit.getLocations()[0]
                            + " start:" + fileSplit.getStart() + " length:" + split.getLength() + " partition:"
                            + partition);
                }
                VertexReader vertexReader = vertexInputFormat.createVertexReader(split, context);
                vertexReader.initialize(split, context);
                Vertex readerVertex = (Vertex) BspUtils.createVertex(conf);
                ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldSize);
                DataOutput dos = tb.getDataOutput();

                /**
                 * set context
                 */
                Context mapperContext = new Mapper().new Context(conf, new TaskAttemptID(), null, null, null, null,
                        splits.get(partition));
                Vertex.setContext(mapperContext);

                /**
                 * empty vertex value
                 */
                Writable emptyVertexValue = (Writable) BspUtils.createVertexValue(conf);

                while (vertexReader.nextVertex()) {
                    readerVertex = vertexReader.getCurrentVertex();
                    tb.reset();
                    if (readerVertex.getVertexId() == null) {
                        throw new IllegalArgumentException("loadVertices: Vertex reader returned a vertex "
                                + "without an id!  - " + readerVertex);
                    }
                    if (readerVertex.getVertexValue() == null) {
                        readerVertex.setVertexValue(emptyVertexValue);
                    }
                    WritableComparable vertexId = readerVertex.getVertexId();
                    vertexId.write(dos);
                    tb.addFieldEndOffset();

                    readerVertex.write(dos);
                    tb.addFieldEndOffset();

                    if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                        if (appender.getTupleCount() <= 0)
                            throw new IllegalStateException("zero tuples in a frame!");
                        FrameUtils.flushFrame(frame, writer);
                        appender.reset(frame, true);
                        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                            throw new IllegalStateException();
                        }
                    }
                }

                vertexReader.close();
                if (appender.getTupleCount() > 0) {
                    FrameUtils.flushFrame(frame, writer);
                }
                System.gc();
            }
        };
    }

}