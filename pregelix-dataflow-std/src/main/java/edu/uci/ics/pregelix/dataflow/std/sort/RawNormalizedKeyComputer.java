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

public final class RawNormalizedKeyComputer {

    public int normalize(byte[] bytes, int start, int length) {
        int nk = 0;
        for (int i = 0; i < 4; i++) {
            nk <<= 8;
            if (i < length) {
                nk += (bytes[start + i] & 0xff);
            }
        }
        return nk ^ Integer.MIN_VALUE;
    }

    public int normalize2(byte[] bytes, int start, int length) {
        int nk = 0;
        for (int i = 4; i < 6; i++) {
            nk <<= 8;
            if (i < length) {
                nk += (bytes[start + i] & 0xff);
            }
        }
        return nk;
    }

    public int normalize4(byte[] bytes, int start, int length) {
        int nk = 0;
        for (int i = 4; i < 8; i++) {
            nk <<= 8;
            if (i < length) {
                nk += (bytes[start + i] & 0xff);
            }
        }
        return nk ^ Integer.MIN_VALUE;
    }
}
