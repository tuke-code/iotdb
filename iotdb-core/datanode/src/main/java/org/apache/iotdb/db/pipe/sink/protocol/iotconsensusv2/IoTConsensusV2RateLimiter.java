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

package org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2;

import org.apache.iotdb.commons.utils.RegionMigrationRateLimiter;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2BatchTransferReq;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferReq;

import java.nio.ByteBuffer;

public class IoTConsensusV2RateLimiter {

  private static final RegionMigrationRateLimiter RATE_LIMITER =
      RegionMigrationRateLimiter.getInstance();

  private IoTConsensusV2RateLimiter() {}

  public static void acquire(TIoTConsensusV2TransferReq req) {
    RATE_LIMITER.acquire(getTransferDataSize(req));
  }

  public static void acquire(TIoTConsensusV2BatchTransferReq req) {
    RATE_LIMITER.acquire(getTransferDataSize(req));
  }

  static long getTransferDataSize(TIoTConsensusV2BatchTransferReq req) {
    return req.getBatchReqs() == null
        ? 0
        : req.getBatchReqs().stream()
            .mapToLong(IoTConsensusV2RateLimiter::getTransferDataSize)
            .sum();
  }

  static long getTransferDataSize(TIoTConsensusV2TransferReq req) {
    return getRemaining(req.body);
  }

  private static int getRemaining(ByteBuffer byteBuffer) {
    return byteBuffer == null ? 0 : byteBuffer.duplicate().remaining();
  }
}
