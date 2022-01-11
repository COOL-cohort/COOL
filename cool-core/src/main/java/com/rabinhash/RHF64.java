/*
 * Copyright 2004 Sean Owen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rabinhash;

import java.security.DigestException;
import java.security.MessageDigestSpi;

/**
 * <p>Provides a {@link java.security.MessageDigest} based on {@link RabinHashFunction64}.</p>
 *
 * @author Sean Owen
 * @version 2.0
 * @since 2.0
 */
public final class RHF64 extends MessageDigestSpi implements Cloneable {

  private final RabinHashFunction64 rhf = RabinHashFunction64.DEFAULT_HASH_FUNCTION;
  private long hash;

  /**
   * @return 8
   */
  protected int engineGetDigestLength() {
    return 8;
  }

  protected void engineUpdate(final byte input) {
    hash = rhf.hash(new byte[]{input}, 0, 1, hash);
  }

  protected void engineUpdate(final byte[] input, final int offset, final int len) {
    hash = rhf.hash(input, offset, len, hash);
  }

  protected byte[] engineDigest() {
    final byte[] buf = new byte[8];
    hashToBuf(buf, 0);
    engineReset();
    return buf;
  }

  /**
   * @param buf    buffer into which to write the digest
   * @param offset offset into buffer at which to start writing
   * @param len    (not used)
   * @return 8
   * @throws DigestException if len is less than 8
   */
  protected int engineDigest(final byte[] buf, final int offset, final int len)
      throws DigestException {
    if (len < 8) {
      throw new DigestException("Output buffer is smaller than digest length of 8");
    }
    hashToBuf(buf, offset);
    engineReset();
    return 8;
  }

  protected void engineReset() {
    hash = 0;
  }

  private void hashToBuf(final byte[] buf, final int offset) {
    buf[offset] = (byte) (hash >> 56);
    buf[offset + 1] = (byte) (hash >> 48);
    buf[offset + 2] = (byte) (hash >> 40);
    buf[offset + 3] = (byte) (hash >> 32);
    buf[offset + 4] = (byte) (hash >> 24);
    buf[offset + 5] = (byte) (hash >> 16);
    buf[offset + 6] = (byte) (hash >> 8);
    buf[offset + 7] = (byte) hash;
  }

}

