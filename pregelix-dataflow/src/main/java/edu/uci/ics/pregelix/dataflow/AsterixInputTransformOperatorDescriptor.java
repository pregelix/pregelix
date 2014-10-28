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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.om.pointables.AListPointable;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.pointables.PointableAllocator;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;

/**
 * This operator transforms Asterix types read from an Asterix LSMBTree to Pregelix Writables
 */
public class AsterixInputTransformOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final int fieldSize = 2;
    private final IConfigurationFactory confFactory;
    private final ARecordType recordType;

    public AsterixInputTransformOperatorDescriptor(JobSpecification spec, RecordDescriptor rDesc, IConfigurationFactory confFactory, ARecordType recordType) {
        super(spec, 1, 1);
        this.recordDescriptors[0] = rDesc;
        this.confFactory = confFactory;
        this.recordType = recordType;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
            throws HyracksDataException {
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            
        	private final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            private final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
            private final ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldSize);
            private final DataOutput dos = tb.getDataOutput();
            private final ByteBuffer writeBuffer = ctx.allocateFrame();
            private final FrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), rd0);
            

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

            @SuppressWarnings({ "rawtypes", "unchecked" })
			@Override
            public void nextFrame(ByteBuffer frame) throws HyracksDataException {	
            	accessor.reset(frame);
            	
                int treeVertexSizeLimit = IterationUtils.getVFrameSize(ctx) / 2;
                int dataflowPageSize = ctx.getFrameSize();
            	
                for (int tIndex = 0; tIndex < accessor.getTupleCount(); tIndex++) {
                	
                    int fldStart = accessor.getTupleStartOffset(tIndex) + accessor.getFieldSlotsLength()
                            + accessor.getFieldStartOffset(tIndex, 0);
                    int fldLen = accessor.getFieldLength(tIndex, 0);
                    
                    /*IPrinter printer = new ARecordPrinterFactory(recordType).createPrinter();
                    ARecordSerializerDeserializer serializer = new ARecordSerializerDeserializer(recordType);
                    
                    Object record = serializer.deserialize(new DataInputStream(new ByteArrayInputStream(accessor.getBuffer().array(), fldStart, fldLen)));
                    try {
						printer.print(accessor.getBuffer().array(), fldStart, fldLen, System.out);
					} catch (AlgebricksException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                    */
                    
                    ARecordPointable recordPointer = (ARecordPointable) new PointableAllocator().allocateRecordValue(recordType);
                    recordPointer.set(accessor.getBuffer().array(), fldStart, fldLen);
                    
                    Configuration conf = confFactory.createConfiguration(ctx);
                    
                    Vertex v = convertPointableToVertex(recordPointer, conf);
                    
                    Writable emptyVertexValue = BspUtils.createVertexValue(conf);
                    
                    if (v.getVertexValue() == null) {
                        v.setVertexValue(emptyVertexValue);
                    }
                    
                    WritableComparable vertexIdWrite = v.getVertexId();
                    try {
                    	tb.reset();
                    	
						vertexIdWrite.write(dos);
						
	                    tb.addFieldEndOffset();

	                    v.write(dos);
	                    
	                    tb.addFieldEndOffset();
	                    
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                    
                    if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                        if (appender.getTupleCount() <= 0) {
                            throw new IllegalStateException("zero tuples in a frame!");
                        }
                        FrameUtils.flushFrame(frame, writer);
                        appender.reset(frame, true);
                        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                            //this place should never be reached, otherwise it is a bug
                            throw new IllegalStateException(
                                    "An overflow vertex content should not be flushed into bulkload dataflow.");
                        }
                    }
                }
            }

            @Override
            public void open() throws HyracksDataException {
                writer.open();
                appender.reset(writeBuffer, true);
            }
            
            
            @SuppressWarnings({ "rawtypes", "unchecked" })
            /**
             * Internal helper function to transform a Asterix ARecordPointable pointer into a Pregelix Vertex
             * 
             * @param pointer
             * @param conf
             * @return
             */
			private Vertex convertPointableToVertex(ARecordPointable pointer, Configuration conf) {
            	
                Vertex v = BspUtils.createVertex(conf);
                
                VLongWritable vertexId = new VLongWritable();
                
                vertexId.set(AInt64SerializerDeserializer.getLong(pointer.getFieldValues().get(0).getByteArray(), pointer.getFieldValues().get(0).getStartOffset() + 1));
                v.setVertexId(vertexId);
                
                v.setVertexValue(vertexId);
                
                AListPointable edges = (AListPointable) pointer.getFieldValues().get(2);
                for(IVisitablePointable edge: edges.getItems()) {
                	ARecordPointable edgePointer = (ARecordPointable) edge;
                	VLongWritable destId = new VLongWritable();
                	destId.set(AInt64SerializerDeserializer.getLong(edgePointer.getFieldValues().get(0).getByteArray(), edgePointer.getFieldValues().get(0).getStartOffset()+1));
                	v.addEdge(destId, null);
                }
                
                return v;
            	
            }

			@Override
			public void close() throws HyracksDataException {
	            if (appender.getTupleCount() > 0) {
	                FrameUtils.flushFrame(writeBuffer, writer);
	            }
				writer.close();
			}

        };
    }
}
