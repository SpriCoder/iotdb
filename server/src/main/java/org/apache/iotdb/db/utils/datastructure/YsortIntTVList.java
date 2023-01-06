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

public class YsortIntTVList extends QuickIntTVList {
  @Override
  public void sort() {
    if (!sorted) {
      ysort(0, rowCount - 1);
    }
    sorted = true;
  }

  public void ysort(int m, int n) {
    if (m >= n) return;
    boolean flag = true, left_flag = false, right_flag = false;
    long mid_key = getTime((m + n) / 2);
    int i = m, j = n, size;
    while (flag) {
      // build the left subfile ensuring that the rightmost key is largest.
      while (getTime(i) < mid_key && i != j) {
        if (i != m) {
          if (getTime(i - 1) > getTime(i)) {
            swap(i - 1, i);
            left_flag = true;
          }
        }
        i += 1;
      }
      // build the right subfile ensuring that the leftmost key is smallest
      while (getTime(j) >= mid_key && i != j) {
        if (j != n) {
          if (getTime(j) > getTime(j + 1)) {
            swap(j + 1, j);
            right_flag = true;
          }
        }
        j--;
      }
      if (i != j) { // interchange i from the left subfile with j from right subfile
        swap(i, j);
      } else {
        // i == j partitioning into left and right subfiles has been completed
        if (getTime(j) >= mid_key) {
          // check the right subfile to ensure the first element, k[j] , the smallest
          if (getTime(j) > getTime(j + 1)) {
            swap(j + 1, j);
            right_flag = true;
          }
        } else {
          if (getTime(i - 1) > getTime(i)) {
            swap(i - 1, i);
            left_flag = true;
          }
          if (getTime(i - 2) > getTime(i - 1)) {
            swap(i - 1, i - 2);
          }
        }
        flag = false;
      }
    }
    // process the left subfile
    size = i - m;
    if (size > 2) { // subfile must have at least 3 elements to process and not already sorted
      if (left_flag) {
        if (size == 3) { // special case of 3 elements; place m and m+1 in sorted order
          if (getTime(m) > getTime(m + 1)) swap(m, m + 1);
        } else ysort(m, i - 2);
      }
    }
    // process the right subfile
    size = n - j + 1;
    if (size > 2) {
      if (right_flag) {
        if (size == 3) {
          if (getTime(j + 1) > getTime(j + 2)) swap(j + 1, j + 2);
        } else ysort(j + 1, n);
      }
    }
  }
}
