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
package edu.uci.ics.pregelix.dataflow.std.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractStateObject;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.pregelix.dataflow.std.group.IClusteredAggregatorDescriptorFactory;

public class FastSortOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int SORT_ACTIVITY_ID = 0;
    private static final int MERGE_ACTIVITY_ID = 1;

    private final int[] sortFields;
    private final int framesLimit;

    private final int[] groupFields;
    private final IClusteredAggregatorDescriptorFactory aggregatorFactory;
    private final IClusteredAggregatorDescriptorFactory partialAggregatorFactory;
    private final RecordDescriptor combinedRecordDesc;
    private final RecordDescriptor outputRecordDesc;
    private final boolean localSide;

    public FastSortOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            RecordDescriptor recordDescriptor, int[] groupFields,
            IClusteredAggregatorDescriptorFactory partialAggregatorFactory,
            IClusteredAggregatorDescriptorFactory aggregatorFactory, RecordDescriptor combinedRecordDesc,
            RecordDescriptor outRecordDesc, boolean localSide) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        this.sortFields = sortFields;
        if (framesLimit <= 1) {
            throw new IllegalStateException();// minimum of 2 fames (1 in,1 out)
        }
        this.recordDescriptors[0] = recordDescriptor;

        this.groupFields = groupFields;
        this.aggregatorFactory = aggregatorFactory;
        this.partialAggregatorFactory = partialAggregatorFactory;
        this.combinedRecordDesc = combinedRecordDesc;
        this.outputRecordDesc = outRecordDesc;
        this.localSide = localSide;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        SortActivity sa = new SortActivity(new ActivityId(odId, SORT_ACTIVITY_ID));
        MergeActivity ma = new MergeActivity(new ActivityId(odId, MERGE_ACTIVITY_ID));

        builder.addActivity(this, sa);
        builder.addSourceEdge(0, sa, 0);

        builder.addActivity(this, ma);
        builder.addTargetEdge(0, ma, 0);

        builder.addBlockingEdge(sa, ma);
    }

    public static class SortTaskState extends AbstractStateObject {
        private List<IFrameReader> runs;
        private IFrameSorter frameSorter;

        public SortTaskState() {
        }

        private SortTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {

        }
    }

    private class SortActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public SortActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private ExternalSortRunGenerator runGen;

                @Override
                public void open() throws HyracksDataException {
                    runGen = new ExternalSortRunGenerator(ctx, sortFields, recordDescriptors[0], framesLimit,
                            groupFields, new IBinaryComparator[] { new RawBinaryComparator() },
                            partialAggregatorFactory, combinedRecordDesc);
                    runGen.open();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    runGen.nextFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    SortTaskState state = new SortTaskState(ctx.getJobletContext().getJobId(), new TaskId(
                            getActivityId(), partition));
                    runGen.close();
                    state.runs = runGen.getRuns();
                    state.frameSorter = runGen.getFrameSorter();
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                    runGen.fail();
                }
            };
            return op;
        }
    }

    private class MergeActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MergeActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            IOperatorNodePushable op = new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    SortTaskState state = (SortTaskState) ctx.getStateObject(new TaskId(new ActivityId(getOperatorId(),
                            SORT_ACTIVITY_ID), partition));
                    List<IFrameReader> runs = state.runs;
                    IFrameSorter frameSorter = state.frameSorter;
                    int necessaryFrames = Math.min(runs.size() + 2, framesLimit);
                    ExternalSortRunMerger merger = new ExternalSortRunMerger(ctx, frameSorter, runs, sortFields,
                            combinedRecordDesc, outputRecordDesc, necessaryFrames, writer, groupFields,
                            new IBinaryComparator[] { new RawBinaryComparator() }, partialAggregatorFactory,
                            aggregatorFactory, localSide);
                    merger.process();
                }
            };
            return op;
        }
    }
}