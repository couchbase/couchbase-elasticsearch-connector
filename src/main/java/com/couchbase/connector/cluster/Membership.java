/*
 * Copyright 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connector.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashSet;
import java.util.Set;

import static com.couchbase.connector.dcp.DcpHelper.allPartitions;
import static com.couchbase.connector.util.ListHelper.chunks;

public class Membership {
  private final int memberNumber; // valid rage = from 1 to clusterSize, inclusive
  private final int clusterSize;

  @JsonCreator
  public static Membership of(@JsonProperty("memberNumber") int memberNumber, @JsonProperty("clusterSize") int clusterSize) {
    return new Membership(memberNumber, clusterSize);
  }

  private Membership(int memberNumber, int clusterSize) {
    if (memberNumber <= 0 || memberNumber > clusterSize) {
      throw new IllegalArgumentException("Invalid static group membership number, must be between 1 and cluster size (" + clusterSize + ") inclusive.");
    }
    this.memberNumber = memberNumber;
    this.clusterSize = clusterSize;
  }

  public int getMemberNumber() {
    return memberNumber;
  }

  public int getClusterSize() {
    return clusterSize;
  }

  public Set<Integer> getPartitions(int numPartitions) {
    return new LinkedHashSet<>(chunks(allPartitions(numPartitions), clusterSize).get(memberNumber - 1));
  }

  @Override
  public String toString() {
    return memberNumber + "/" + clusterSize;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Membership that = (Membership) o;

    if (memberNumber != that.memberNumber) {
      return false;
    }
    return clusterSize == that.clusterSize;
  }

  @Override
  public int hashCode() {
    int result = memberNumber;
    result = 31 * result + clusterSize;
    return result;
  }
}
