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

import static org.apache.iotdb.db.rescon.PrimitiveArrayManager.ARRAY_SIZE;

import java.util.ArrayList;
import java.util.List;

public class CKSortIntTBVList extends QuickIntTVList {
  protected List<long[]> tsOrder = new ArrayList<>();
  protected List<int[]> vsOrder = new ArrayList<>();
  protected int orderLen = 0;
  protected List<long[]> tsDisorder = new ArrayList<>();
  protected List<int[]> vsDisorder = new ArrayList<>();
  protected int disorderLen = 0;

  @Override
  public void sort() {
    if (!sorted && rowCount >= 1) {
      CKSort();
      clearSorted();
    }
    sorted = true;
  }

  private void clearSorted() {
    tsDisorder.clear();
    tsOrder.clear();
    vsDisorder.clear();
    vsOrder.clear();
  }

  protected long getTs(List<long[]> ts, int index) {
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return ts.get(arrayIndex)[elementIndex];
  }

  protected int getVs(List<int[]> vs, int index) {
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return vs.get(arrayIndex)[elementIndex];
  }

  protected void setDisorder(int index, long t, int v) {
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    tsDisorder.get(arrayIndex)[elementIndex] = t;
    vsDisorder.get(arrayIndex)[elementIndex] = v;
  }

  protected void addOrder(long t, int v) {
    int arrayIndex = orderLen / ARRAY_SIZE;
    int elementIndex = orderLen % ARRAY_SIZE;
    if (elementIndex == 0) {
      tsOrder.add(new long[ARRAY_SIZE]);
      vsOrder.add(new int[ARRAY_SIZE]);
    }
    tsOrder.get(arrayIndex)[elementIndex] = t;
    vsOrder.get(arrayIndex)[elementIndex] = v;
    orderLen++;
  }

  protected void addDisorder(long t, int v) {
    int arrayIndex = disorderLen / ARRAY_SIZE;
    int elementIndex = disorderLen % ARRAY_SIZE;
    if (elementIndex == 0) {
      tsDisorder.add(new long[ARRAY_SIZE]);
      vsDisorder.add(new int[ARRAY_SIZE]);
    }
    tsDisorder.get(arrayIndex)[elementIndex] = t;
    vsDisorder.get(arrayIndex)[elementIndex] = v;
    disorderLen++;
  }

  protected void swapDisorder(int p, int q) {
    long tp = getTs(tsDisorder, p);
    long tq = getTs(tsDisorder, q);
    int vp = getVs(vsDisorder, p);
    int vq = getVs(vsDisorder, q);
    setDisorder(p, tq, vq);
    setDisorder(q, tp, vp);
  }

  public void CKSort() {
    int i = 0;
    while (i < rowCount) {
      long t = getTime(i);
      int v = getInt(i);
      if ((orderLen != 0) && getTs(tsOrder, orderLen - 1) > t) {
        addDisorder(getTs(tsOrder, orderLen - 1), getVs(vsOrder, orderLen - 1));
        addDisorder(t, v);
        orderLen--;
      } else {
        addOrder(t, v);
      }
      i++;
    }
    // sort the disorder pairs
    QSort(0, disorderLen - 1);
    // merge the tsOrder and ts2 back to timestamps
    i = 0;
    int a = 0, b = 0;
    while (a < orderLen && b < disorderLen) {
      long ta = getTs(tsOrder, a), tb = getTs(tsDisorder, b);
      if (ta <= tb) {
        set(i, ta, getVs(vsOrder, a));
        a++;
      } else {
        set(i, tb, getVs(vsDisorder, b));
        b++;
      }
      i++;
    }
    while (a < orderLen) {
      set(i++, getTs(tsOrder, a), getVs(vsOrder, a));
      a++;
    }
    while (b < disorderLen) {
      set(i++, getTs(tsDisorder, b), getVs(vsDisorder, b));
      b++;
    }
  }

  public int partition(int low, int high) {
    int left = low, right = high, pIndex = (low + high) / 2;
    long pivot = getTs(tsDisorder, pIndex);
    while (getTs(tsDisorder, left) < pivot) {
      left++;
    }
    while (getTs(tsDisorder, right) > pivot) {
      right--;
    }
    for (int j = left; j <= right; j++) {
      // If current element is greater than or equal to pivot
      if (getTs(tsDisorder, j) < pivot) {
        // swap arr[i] and arr[j]
        swapDisorder(left, j);
        if (pIndex == left) {
          pIndex = j;
        }
        left++;
      }
    }
    if (left != pIndex) {
      swapDisorder(left, pIndex);
      pIndex = left;
    }
    return pIndex;
  }

  private void QSort(int low, int high) {
    if (low < high) {
      /* pi is partitioning index, arr[pi] is now at right place */
      int pi = partition(low, high);
      // Recursively sort elements before partition and after partition
      QSort(low, pi - 1);
      QSort(pi + 1, high);
    }
  }

  @Override
  protected void set(int src, int dest) {
    long srcT = getTime(src);
    int srcV = getInt(src);
    set(dest, srcT, srcV);
  }
}
