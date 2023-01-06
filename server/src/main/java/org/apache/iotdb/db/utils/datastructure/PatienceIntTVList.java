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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.rescon.PrimitiveArrayManager.ARRAY_SIZE;

public class PatienceIntTVList extends QuickIntTVList {

  private ArrayList<ArrayList<int[]>> valueLists = new ArrayList<>();
  private ArrayList<ArrayList<long[]>> timestampLists = new ArrayList<>();

  private ArrayList<Long> tails = new ArrayList<>();

  private ArrayList<Integer> rowCounts = new ArrayList<>();

  private long[] sortedTimestamps;
  private int[] sortedValues;

  @Override
  public void sort() {
    patienceSort();
    mergeRuns();
    clearSorted();
  }

  public void patienceSort() {
    int curElemIndex = 0;
    while (curElemIndex < rowCount) {
      int arrayIndex = curElemIndex / ARRAY_SIZE;
      int elementIndex = curElemIndex % ARRAY_SIZE;
      long t = timestamps.get(arrayIndex)[elementIndex];
      int v = values.get(arrayIndex)[elementIndex];
      int k = binarySearchListIndex(t);
      if (k == -1) {
        // new sorted runs
        timestampLists.add(new ArrayList<>());
        valueLists.add(new ArrayList<>());
        rowCounts.add(0);
        tails.add(Long.MIN_VALUE);
        // add current element to kth run
        add(timestampLists.size() - 1, t, v);
        curElemIndex += 1;
      } else {
        // add current element to kth run
        add(k, t, v);
        // iterate until less than
        curElemIndex += 1;
        while (curElemIndex < rowCount) {
          arrayIndex = curElemIndex / ARRAY_SIZE;
          elementIndex = curElemIndex % ARRAY_SIZE;
          t = timestamps.get(arrayIndex)[elementIndex];
          v = values.get(arrayIndex)[elementIndex];
          if (t < tails.get(k)) break;
          // add elements to kth runs
          add(k, t, v);
          curElemIndex += 1;
        }
      }
    }
  }

  private void add(int index, long timestamp, int value) {
    // System.out.printf("index=%d\n", index);
    List<long[]> tss = timestampLists.get(index);
    List<int[]> vss = valueLists.get(index);
    int rowCount = rowCounts.get(index);
    if (rowCount % ARRAY_SIZE == 0) {
      tss.add(new long[ARRAY_SIZE]);
      vss.add(new int[ARRAY_SIZE]);
    }
    int arrayIndex = rowCount / ARRAY_SIZE;
    int elementIndex = rowCount % ARRAY_SIZE;

    tss.get(arrayIndex)[elementIndex] = timestamp;
    vss.get(arrayIndex)[elementIndex] = value;

    tails.set(index, timestamp);
    rowCounts.set(index, rowCount + 1);
  }

  private void mergeRuns() {
    Map<Integer, Integer> map = new HashMap<>();
    for (int i = 0; i < rowCounts.size(); i++) {
      map.put(i, rowCounts.get(i));
    }
    Map<Integer, Integer> sorted =
        map.entrySet().stream()
            .sorted(Map.Entry.comparingByValue())
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

    HashMap<Integer, long[]> tss = new HashMap<>();
    long[] t1 = new long[rowCount];
    long[] t2 = new long[rowCount];
    tss.put(1, t1);
    tss.put(2, t2);
    int[] v2 = new int[rowCount];
    int[] v1 = new int[rowCount];
    //
    HashMap<Integer, int[]> vss = new HashMap<>();
    vss.put(1, v1);
    vss.put(2, v2);
    int idx1 = 0;
    int nextEmptyArrayLoc = 0;
    List<HashMap<String, Integer>> elemRuns = new ArrayList<>();
    //        moveTimes += rowCount;
    for (int idx : sorted.keySet()) {
      ArrayList<long[]> ts = timestampLists.get(idx);
      ArrayList<int[]> vs = valueLists.get(idx);
      int rowCount = rowCounts.get(idx);

      // copy the array into t1
      int j = 0, elementIndex = 0, arrayIndex = 0;
      while (j < rowCount) {
        v1[idx1] = vs.get(arrayIndex)[elementIndex];
        t1[idx1++] = ts.get(arrayIndex)[elementIndex];
        j += 1;
        elementIndex += 1;
        if (elementIndex == ARRAY_SIZE) {
          elementIndex = 0;
          arrayIndex += 1;
        }
      }
      // record elements runs
      HashMap<String, Integer> hashMap = new HashMap<>();
      hashMap.put("arr", 1); // 初始时都是放在t1中
      hashMap.put("start", nextEmptyArrayLoc);
      hashMap.put("size", rowCount);
      elemRuns.add(hashMap);
      nextEmptyArrayLoc += rowCount;
    }
    int i = 0, start_index = 0;
    while (elemRuns.size() > 1) {
      if (i >= elemRuns.size() - 1) {
        i = 0;
        start_index = 0;
      } else if (elemRuns.get(0).get("size") + elemRuns.get(1).get("size")
          < elemRuns.get(i).get("size") + elemRuns.get(i + 1).get("size")) {
        i = 0;
        start_index = 0;
      }

      int key1 = elemRuns.get(i).get("arr"), len1 = elemRuns.get(i).get("size");
      int key2 = elemRuns.get(i + 1).get("arr"), len2 = elemRuns.get(i + 1).get("size");

      int sIdx1 = start_index;
      int sIdx2 = sIdx1 + len1;
      int sIdxD = start_index;
      int eIdx1 = sIdx1 + len1;
      int eIdx2 = eIdx1 + len2;
      start_index += len1 + len2;
      // only ensure r1 and dest is not the same array.

      int keyd = 1;
      if (key1 == 1) keyd = 2;

      // from key1[sIdx1:sIdx1+len1) and key2[sIdx2:sIdx2+len2) to dest[sIdxD:start_index+len1+len2]
      // note the key1 and dest must be different
      long[] ts1 = tss.get(key1), ts2 = tss.get(key2), tsd = tss.get(keyd);
      int[] vs1 = vss.get(key1), vs2 = vss.get(key2), vsd = vss.get(keyd);
      while (sIdx1 < eIdx1 && sIdx2 < eIdx2) {
        if (ts2[sIdx2] < ts1[sIdx1]) {
          vsd[sIdxD] = vs2[sIdx2];
          tsd[sIdxD++] = ts2[sIdx2++];
        } else {
          vsd[sIdxD] = vs1[sIdx1];
          tsd[sIdxD++] = ts1[sIdx1++];
        }
        //                moveTimes += 1;
      }
      // moves remaining moves; when key2==keyd, no need for moves;
      if (sIdx1 < eIdx1)
        while (sIdx1 < eIdx1) {
          vsd[sIdxD] = vs1[sIdx1];
          tsd[sIdxD++] = ts1[sIdx1++];
          //                    moveTimes += 1;
        }
      if (sIdx2 < eIdx2 && key2 != keyd)
        while (sIdx2 < eIdx2) {
          vsd[sIdxD] = vs2[sIdx2];
          tsd[sIdxD++] = ts2[sIdx2++];
          //                    moveTimes += 1;
        }

      elemRuns.remove(i + 1);
      elemRuns.get(i).replace("arr", keyd);
      elemRuns.get(i).replace("size", len1 + len2);
      i += 1;
    }
    sortedTimestamps = tss.get(elemRuns.get(0).get("arr"));
    sortedValues = vss.get(elemRuns.get(0).get("arr"));
  }

  private int binarySearchListIndex(long timestamp) {
    int lo = 0;
    int hi = tails.size() - 1;
    if (hi < lo) return -1;
    while (lo < hi) {
      int mid = (lo + hi) / 2;
      long t = tails.get(mid);
      if (t < timestamp) hi = mid;
      else if (t == timestamp) return mid;
      else lo = mid + 1;
    }
    if (hi == tails.size() - 1 && tails.get(hi) > timestamp) return -1; // new runs
    else return hi;
  }

  private void clearSorted() {
    for (int i = 0; i < rowCount; i++) {
      set(i, sortedTimestamps[i], sortedValues[i]);
    }
    sortedTimestamps = null;
    sortedValues = null;
    //        System.gc();
  }
}
