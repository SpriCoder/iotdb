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

public class CKSortIntTVList extends QuickIntTVList {

  private long[] tsDisorder;
  private int[] vsDisorder;
  private int disorderLen;

  private long[] tsOrder;
  private int[] vsOrder;
  private int orderLen;

  @Override
  public void sort() {
    if (!sorted && rowCount >= 1) {
      CKSort();
    }
    sorted = true;
  }

  public void CKSort() {
    // construct the array
    tsOrder = new long[rowCount];
    vsOrder = new int[rowCount];
    orderLen = 0;
    tsDisorder = new long[rowCount];
    vsDisorder = new int[rowCount];
    disorderLen = 0;
    int i = 0;
    while (i < rowCount) {
      long t = getTime(i);
      int v = getInt(i);
      if ((orderLen != 0) && tsOrder[orderLen - 1] > t) {
        tsDisorder[disorderLen] = tsOrder[orderLen - 1];
        vsDisorder[disorderLen] = vsOrder[orderLen - 1];
        disorderLen++;
        orderLen--;

        tsDisorder[disorderLen] = t;
        vsDisorder[disorderLen] = v;
        disorderLen++;
      } else {
        tsOrder[orderLen] = t;
        vsOrder[orderLen] = v;
        orderLen++;
      }
      i++;
    }
    // sort the disorder pairs
    QSort(0, disorderLen - 1);
    // merge the tsOrder and ts2 back to timestamps
    i = 0;
    int a = 0, b = 0;
    while (a < orderLen && b < disorderLen) {
      if (tsOrder[a] <= tsDisorder[b]) {
        set(i, tsOrder[a], vsOrder[a]);
        a++;
      } else {
        set(i, tsDisorder[b], vsDisorder[b]);
        b++;
      }
      i++;
    }
    while (a < orderLen) {
      set(i++, tsOrder[a], vsOrder[a]);
      a++;
    }
    while (b < disorderLen) {
      set(i++, tsDisorder[b], vsDisorder[b]);
      b++;
    }
    tsOrder = null;
    vsOrder = null;
    tsDisorder = null;
    vsDisorder = null;
  }

  public int partition(int low, int high) {
    int left = low, right = high, pIndex = (low + high) / 2;
    long pivot = tsDisorder[pIndex];
    while (tsDisorder[left] < pivot) {
      left++;
    }
    while (tsDisorder[right] > pivot) {
      right--;
    }
    for (int j = left; j <= right; j++) {
      // If current element is greater than or equal to pivot
      if (tsDisorder[j] < pivot) {
        // swap arr[i] and arr[j]
        long t = tsDisorder[left];
        tsDisorder[left] = tsDisorder[j];
        tsDisorder[j] = t;
        //                int v = vsDisorder[left];
        //                vsDisorder[left] = vsDisorder[j];
        //                vsDisorder[j] = v;
        if (pIndex == left) {
          pIndex = j;
        }
        left++;
      }
    }
    if (left != pIndex) {
      long t = tsDisorder[left];
      tsDisorder[left] = tsDisorder[pIndex];
      tsDisorder[pIndex] = t;
      //            int v = vsDisorder[left];
      //            vsDisorder[left] = vsDisorder[pIndex];
      //            vsDisorder[pIndex] = v;
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
