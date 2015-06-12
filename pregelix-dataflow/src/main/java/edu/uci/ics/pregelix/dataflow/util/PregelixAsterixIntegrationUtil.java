package edu.uci.ics.pregelix.dataflow.util;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.ADouble;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.pregelix.api.datatypes.BooleanWritable;
import edu.uci.ics.pregelix.api.datatypes.DoubleWritable;
import edu.uci.ics.pregelix.api.datatypes.FloatWritable;
import edu.uci.ics.pregelix.api.datatypes.IntWritable;
import edu.uci.ics.pregelix.api.datatypes.LongWritable;
import edu.uci.ics.pregelix.api.datatypes.NullWritable;
import edu.uci.ics.pregelix.api.datatypes.VIntWritable;
import edu.uci.ics.pregelix.api.datatypes.VLongWritable;

public class PregelixAsterixIntegrationUtil {

    /**
     * @TODO: Move somewhere else
     * @TODO: Exception handling
     * @TODO: Implement a pool for the types
     * @param bytes
     * @param types
     */
    public static Writable transformStateFromAsterix(byte[] bytes, ATypeTag type, int offset) {
        switch (type) {
            case DOUBLE: {
                return new DoubleWritable(ADoubleSerializerDeserializer.getDouble(bytes, offset));
            }
            case FLOAT: {
                return new FloatWritable(AFloatSerializerDeserializer.getFloat(bytes, offset));
            }
            case BOOLEAN: {
                return new BooleanWritable(ABooleanSerializerDeserializer.getBoolean(bytes, offset));
            }
            case INT32: {
                return new IntWritable(AInt32SerializerDeserializer.getInt(bytes, offset));
            }
            case INT64: {
                return new VLongWritable(AInt64SerializerDeserializer.getLong(bytes, offset));
            }
            case NULL: {
                return NullWritable.get();
            }
            case STRING:
            case INT8:
            case INT16:
            case CIRCLE:
            case DATE:
            case DATETIME:
            case LINE:
            case TIME:
            case DURATION:
            case YEARMONTHDURATION:
            case DAYTIMEDURATION:
            case INTERVAL:
            case ORDEREDLIST:
            case POINT:
            case POINT3D:
            case RECTANGLE:
            case POLYGON:
            case RECORD:
            case UNORDEREDLIST:
            case UUID:
            default: {
                throw new NotImplementedException("No type transformation implemented for type " + type + " .");
            }
        }
    }

    public static Writable transformStateFromAsterixDefaults(ATypeTag type) {
        switch (type) {
            case DOUBLE: {
                return new DoubleWritable(0.0);
            }
            case FLOAT: {
                return new FloatWritable(0.0f);
            }
            case BOOLEAN: {
                return new BooleanWritable(false);
            }
            case INT32: {
                return new IntWritable(0);
            }
            case INT64: {
                return new VLongWritable(0l);
            }
            case NULL: {
                return NullWritable.get();
            }
            case STRING:
            case INT8:
            case INT16:
            case CIRCLE:
            case DATE:
            case DATETIME:
            case LINE:
            case TIME:
            case DURATION:
            case YEARMONTHDURATION:
            case DAYTIMEDURATION:
            case INTERVAL:
            case ORDEREDLIST:
            case POINT:
            case POINT3D:
            case RECTANGLE:
            case POLYGON:
            case RECORD:
            case UNORDEREDLIST:
            case UUID:
            default: {
                throw new NotImplementedException("No type transformation implemented for type " + type + " .");
            }
        }
    }

    /**
     * @TODO: Move somewhere else
     * @TODO: Exception handling
     * @TODO: Implement a pool for the types
     * @param bytes
     * @param types
     */
    public static IAObject transformStateToAsterix(Writable value) {

        if (value instanceof DoubleWritable) {
            return new ADouble(((DoubleWritable) value).get());
        } else if (value instanceof org.apache.hadoop.io.DoubleWritable) {
            return new ADouble(((org.apache.hadoop.io.DoubleWritable) value).get());
        } else if (value instanceof FloatWritable) {
            return new AFloat(((FloatWritable) value).get());
        } else if (value instanceof org.apache.hadoop.io.FloatWritable) {
            return new AFloat(((org.apache.hadoop.io.FloatWritable) value).get());
        } else if (value instanceof BooleanWritable) {
            if (((BooleanWritable) value).get()) {
                return ABoolean.TRUE;
            } else {
                return ABoolean.FALSE;
            }
        } else if (value instanceof org.apache.hadoop.io.BooleanWritable) {
            if (((org.apache.hadoop.io.BooleanWritable) value).get()) {
                return ABoolean.TRUE;
            } else {
                return ABoolean.FALSE;
            }
        } else if (value instanceof IntWritable) {
            return new AInt32(((IntWritable) value).get());
        } else if (value instanceof VIntWritable) {
            return new AInt32(((VIntWritable) value).get());
        } else if (value instanceof org.apache.hadoop.io.IntWritable) {
            return new AInt32(((org.apache.hadoop.io.IntWritable) value).get());
        } else if (value instanceof org.apache.hadoop.io.VIntWritable) {
            return new AInt32(((org.apache.hadoop.io.VIntWritable) value).get());
        } else if (value instanceof LongWritable) {
            return new AInt64(((LongWritable) value).get());
        } else if (value instanceof VLongWritable) {
            return new AInt64(((VLongWritable) value).get());
        } else if (value instanceof org.apache.hadoop.io.LongWritable) {
            return new AInt64(((org.apache.hadoop.io.LongWritable) value).get());
        } else if (value instanceof org.apache.hadoop.io.VLongWritable) {
            return new AInt64(((org.apache.hadoop.io.VLongWritable) value).get());
        } else if (value instanceof NullWritable) {
            return ANull.NULL;
        } else if (value instanceof org.apache.hadoop.io.NullWritable) {
            return ANull.NULL;
        } else {
            throw new NotImplementedException("No type transformation implemented for writable "
                    + value.getClass().getName() + " .");
        }
    }

}
