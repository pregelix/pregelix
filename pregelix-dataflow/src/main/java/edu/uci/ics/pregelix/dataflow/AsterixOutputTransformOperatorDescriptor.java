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
package edu.uci.ics.pregelix.dataflow;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AUnorderedList;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameDeserializer;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.pregelix.api.datatypes.VLongWritable;
import edu.uci.ics.pregelix.api.graph.Edge;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRecordDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.util.PregelixAsterixIntegrationUtil;

/**
 * This operator transforms Asterix types read from an Asterix LSMBTree to Pregelix Writables
 */
public class AsterixOutputTransformOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final IRecordDescriptorFactory inputRdFactory;
    private final ARecordType recordType;

    public AsterixOutputTransformOperatorDescriptor(JobSpecification spec, RecordDescriptor rDesc,
            IConfigurationFactory confFactory, IRecordDescriptorFactory inputRdFactory, ARecordType recordType) {
        super(spec, 1, 1);
        this.recordDescriptors[0] = rDesc;
        this.inputRdFactory = inputRdFactory;
        this.recordType = recordType;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
            throws HyracksDataException {
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            private RecordDescriptor rd0;
            private FrameDeserializer frameDeserializer;
            private ClassLoader ctxCL;
          
            private final ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
            private final DataOutput dos = tb.getDataOutput();

            private final ByteBuffer writeBuffer = ctx.allocateFrame();

            private final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());

            @Override
            public void open() throws HyracksDataException {
                rd0 = inputRdFactory == null ? recordDescProvider.getInputRecordDescriptor(getActivityId(), 0)
                        : inputRdFactory.createRecordDescriptor(ctx);
                frameDeserializer = new FrameDeserializer(ctx.getFrameSize(), rd0);
                ctxCL = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

                writer.open();
                appender.reset(writeBuffer, true);

            }

            @SuppressWarnings({ "unchecked", "rawtypes" })
            @Override
            public void nextFrame(ByteBuffer frame) throws HyracksDataException {
                frameDeserializer.reset(frame);
                try {
                    while (!frameDeserializer.done()) {

                        Object[] tuple = frameDeserializer.deserializeRecord();
                        Vertex v = (Vertex) tuple[1];

                        AInt64 vertexId = new AInt64(((VLongWritable) v.getVertexId()).get());

                        ArrayList<IAObject> edgeList = new ArrayList<IAObject>();

                        List<Edge> l = v.getEdges();
                        for (Edge e : l) {
                            AInt64 destId = new AInt64(((VLongWritable) e.getDestVertexId()).get());
                            IAObject[] fields = new IAObject[2];
                            fields[0] = destId;
                            fields[1] = PregelixAsterixIntegrationUtil.transformStateToAsterix(e.getEdgeValue());
                            ARecord edgeRecord = new ARecord(
                                    (ARecordType) ((AUnorderedListType) recordType.getFieldTypes()[2]).getItemType(),
                                    fields);
                            edgeList.add(edgeRecord);
                        }

                        AUnorderedList edgeContainer = new AUnorderedList(
                                (AUnorderedListType) recordType.getFieldTypes()[2], edgeList);

                        IAObject[] fields = new IAObject[3];
                        fields[0] = vertexId;
                        fields[1] = PregelixAsterixIntegrationUtil.transformStateToAsterix(v.getVertexValue());
                        fields[2] = edgeContainer;
                        ARecord record = new ARecord(recordType, fields);

                        tb.reset();

                        AInt64SerializerDeserializer.INSTANCE.serialize(vertexId, dos);
                        tb.addFieldEndOffset();
                        
                        dos.write(new byte[]{24});
                        ARecordSerializerDeserializer ser = new ARecordSerializerDeserializer(recordType);
                        ser.serialize(record, dos, true);

                        tb.addFieldEndOffset();

                        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                            FrameUtils.flushFrame(writeBuffer, writer);
                            appender.reset(writeBuffer, true);
                            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                throw new IllegalStateException();
                            }
                        }
                    }

                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            

            @Override
            public void fail() throws HyracksDataException {
                Thread.currentThread().setContextClassLoader(ctxCL);
            }

            @Override
            public void close() throws HyracksDataException {
                try {
                    if (appender.getTupleCount() > 0) {
                        FrameUtils.flushFrame(writeBuffer, writer);
                    }
                    writer.close();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }
}
