/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.utils.datastructure;

public class CKSortIntTVList extends IntTVList {

  @Override
  public void sort() {
    if (!sorted && rowCount >= 1) {
      CKSort();
    }
    sorted = true;
  }

  public void CKSort() {
    // construct the array
    long[] ts = new long[rowCount];
    int[] vs = new int[rowCount];
    int orderLen = 0;
    long[] tsDisorder = new long[rowCount];
    int[] vsDisorder = new int[rowCount];
    int disorderLen = 0;
    int i = 0;
    while (i < rowCount) {
      long t = getTime(i);
      int v = getInt(i);
      if ((orderLen != 0) && ts[orderLen - 1] > t) {
        tsDisorder[disorderLen] = ts[orderLen - 1];
        vsDisorder[disorderLen] = vs[orderLen - 1];
        disorderLen++;
        orderLen--;

        tsDisorder[disorderLen] = t;
        vsDisorder[disorderLen] = v;
        disorderLen++;
      } else {
        ts[orderLen] = t;
        vs[orderLen] = v;
        orderLen++;
      }
      i++;
    }
    // sort the disorder pairs
    QSort(tsDisorder, vsDisorder, 0, disorderLen - 1);
    // merge the ts and ts2 back to timestamps
    i = 0;
    int a = 0, b = 0;
    while (a < orderLen && b < disorderLen) {
      if (ts[a] <= tsDisorder[b]) {
        set(i, ts[a], vs[a]);
        a++;
      } else {
        set(i, tsDisorder[b], vsDisorder[b]);
        b++;
      }
      i++;
    }
    while (a < orderLen) {
      set(i++, ts[a], vs[a]);
      a++;
    }
    while (b < disorderLen) {
      set(i++, tsDisorder[b], vsDisorder[b]);
      b++;
    }
    ts = null;
  }

  private int partition(long[] ts, int[] vs, int low, int high) {
    int gIndex = 0, pIndex = (low + high) / 2;
    long pivot = ts[pIndex];
    while (ts[gIndex] < pivot) {
      gIndex++;
    }
    for (int j = gIndex; j <= high; j++) {
      // If current element is greater than or equal to pivot
      if (ts[j] < pivot) {
        // swap arr[i] and arr[j]
        long t = ts[gIndex];
        ts[gIndex] = ts[j];
        ts[j] = t;
        int v = vs[gIndex];
        vs[gIndex] = vs[j];
        vs[j] = v;
        if (pIndex == gIndex) {
          pIndex = j;
        }
        gIndex++;
      }
    }
    if (gIndex != pIndex) {
      long t = ts[gIndex];
      ts[gIndex] = ts[pIndex];
      ts[pIndex] = t;
      int v = vs[gIndex];
      vs[gIndex] = vs[pIndex];
      vs[pIndex] = v;
      pIndex = gIndex;
    }
    return pIndex;
  }

  private void QSort(long[] ts, int[] vs, int low, int high) {
    if (low < high) {
      /* pi is partitioning index, arr[pi] is now at right place */
      int pi = partition(ts, vs, low, high);
      // Recursively sort elements before partition and after partition
      QSort(ts, vs, low, pi - 1);
      QSort(ts, vs, pi + 1, high);
    }
  }

  @Override
  protected void set(int src, int dest) {
    long srcT = getTime(src);
    int srcV = getInt(src);
    set(dest, srcT, srcV);
  }
}
