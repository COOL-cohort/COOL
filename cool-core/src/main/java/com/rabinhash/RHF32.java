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
 * <p>Provides a {@link java.security.MessageDigest} based on {@link RabinHashFunction32}.</p>
 *
 * @author Sean Owen
 * @version 2.0
 * @since 2.0
 */
public final class RHF32 extends MessageDigestSpi implements Cloneable {

  private final RabinHashFunction32 rhf = RabinHashFunction32.DEFAULT_HASH_FUNCTION;
  private int hash;

  /**
   * @return 4
   */
  protected int engineGetDigestLength() {
    return 4;
  }

  protected void engineUpdate(final byte input) {
    hash = rhf.hash(new byte[]{input}, 0, 1, hash);
  }

  protected void engineUpdate(final byte[] input, final int offset, final int len) {
    hash = rhf.hash(input, offset, len, hash);
  }

  protected byte[] engineDigest() {
    final byte[] buf = new byte[4];
    hashToBuf(buf, 0);
    engineReset();
    return buf;
  }

  /**
   * @param buf    buffer into which to write the digest
   * @param offset offset into buffer at which to start writing
   * @param len    (not used)
   * @return 4
   * @throws DigestException if len is less than 4
   */
  protected int engineDigest(final byte[] buf, final int offset, final int len)
      throws DigestException {
    if (len < 4) {
      throw new DigestException("Output buffer is smaller than digest length of 4");
    }
    hashToBuf(buf, offset);
    engineReset();
    return 4;
  }

  protected void engineReset() {
    hash = 0;
  }

  private void hashToBuf(final byte[] buf, final int offset) {
    buf[offset] = (byte) (hash >> 24);
    buf[offset + 1] = (byte) (hash >> 16);
    buf[offset + 2] = (byte) (hash >> 8);
    buf[offset + 3] = (byte) hash;
  }

}

