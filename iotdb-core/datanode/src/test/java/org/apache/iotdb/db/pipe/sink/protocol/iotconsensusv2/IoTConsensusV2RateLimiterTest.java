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

import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2BatchTransferReq;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferReq;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class IoTConsensusV2RateLimiterTest {

  @Test
  public void testGetTransferDataSizeFromRequestBody() {
    final ByteBuffer body = ByteBuffer.wrap(new byte[8]);
    body.position(2);
    body.limit(6);

    final TIoTConsensusV2TransferReq req = new TIoTConsensusV2TransferReq();
    req.body = body;

    Assert.assertEquals(4, IoTConsensusV2RateLimiter.getTransferDataSize(req));
    Assert.assertEquals(2, body.position());
    Assert.assertEquals(6, body.limit());
    Assert.assertEquals(
        0, IoTConsensusV2RateLimiter.getTransferDataSize(new TIoTConsensusV2TransferReq()));
  }

  @Test
  public void testGetTransferDataSizeFromBatchRequest() {
    final TIoTConsensusV2TransferReq req1 = new TIoTConsensusV2TransferReq();
    req1.body = ByteBuffer.wrap(new byte[3]);
    final TIoTConsensusV2TransferReq req2 = new TIoTConsensusV2TransferReq();
    req2.body = ByteBuffer.wrap(new byte[5]);

    Assert.assertEquals(
        8,
        IoTConsensusV2RateLimiter.getTransferDataSize(
            new TIoTConsensusV2BatchTransferReq(Arrays.asList(req1, req2))));
    Assert.assertEquals(
        0, IoTConsensusV2RateLimiter.getTransferDataSize(new TIoTConsensusV2BatchTransferReq()));
  }
}
