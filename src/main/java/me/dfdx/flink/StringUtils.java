package me.dfdx.flink;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringUtils {

    private static final int HIGH_BIT = 0x1 << 7;

    private static final int HIGH_BIT14 = 0x1 << 14;

    private static final int HIGH_BIT21 = 0x1 << 21;

    private static final int HIGH_BIT28 = 0x1 << 28;

    private static final int SHORT_STRING_MAX_LENGTH = 32 * 1024;

    private static final int STRING_BUFFER_SIZE = 1024;

    // thread-local buffer to read/write bytes from/to the input stream:
    // 1. Having a single preallocated buffer helps to avoid heap allocations in hot path.
    // 2. Buffer is used to read byte[] chunks from the stream.
    // 3. Buffer size is fixed to STRING_BUFFER_SIZE characters plus varint-encoded buffer size value:
    //    it's large enough to play well with CPU internal parallelism, but small enough to have
    //    no noticeable memory overhead.
    private static ThreadLocal<byte[]> stringByteBuffer = ThreadLocal.withInitial(() -> new byte[STRING_BUFFER_SIZE]);

    // thread-local buffer to save on allocations in readString, while doing string decoding from the byte buffer:
    // 1. For reasonably-sized strings (with length < SHORT_STRING_MAX_LENGTH) we can skip the
    //    internal char buffer allocation. Default threshold should fit most of the popular use cases.
    // 2. Otherwise we are going to allocate separate buffer on each invocation.
    private static ThreadLocal<char[]> shortStringCharBuffer = ThreadLocal.withInitial(() -> new char[SHORT_STRING_MAX_LENGTH]);

    public static final void writeString(CharSequence cs, DataOutput out) throws IOException {
        if (cs != null) {
            int strlen = cs.length();

            // the length we write is offset by one, because a length of zero indicates a null value
            int lenToWrite = strlen + 1;
            if (lenToWrite < 0) {
                throw new IllegalArgumentException("CharSequence is too long.");
            }


            int position = 0;
            byte[] buffer = stringByteBuffer.get();
            // string is prefixed by it's variable length encoded size, which can take 1-5 bytes.
            if (lenToWrite < HIGH_BIT) {
                buffer[position++] = (byte) lenToWrite;
            } else if (lenToWrite < HIGH_BIT14) {
                buffer[position++] = (byte)(lenToWrite | HIGH_BIT);
                buffer[position++] = (byte)((lenToWrite >>> 7));
            } else if (lenToWrite < HIGH_BIT21) {
                buffer[position++] = (byte)(lenToWrite | HIGH_BIT);
                buffer[position++] = (byte)((lenToWrite >>> 7) | HIGH_BIT);
                buffer[position++] = (byte)((lenToWrite >>> 14));
            } else if (lenToWrite < HIGH_BIT28) {
                buffer[position++] = (byte)(lenToWrite | HIGH_BIT);
                buffer[position++] = (byte)((lenToWrite >>> 7) | HIGH_BIT);
                buffer[position++] = (byte)((lenToWrite >>> 14) | HIGH_BIT);
                buffer[position++] = (byte)((lenToWrite >>> 21));
            } else {
                buffer[position++] = (byte)(lenToWrite | HIGH_BIT);
                buffer[position++] = (byte)((lenToWrite >>> 7) | HIGH_BIT);
                buffer[position++] = (byte)((lenToWrite >>> 14) | HIGH_BIT);
                buffer[position++] = (byte)((lenToWrite >>> 21) | HIGH_BIT);
                buffer[position++] = (byte)((lenToWrite >>> 28));
            }

            // write the char data, variable length encoded
            for (int i = 0; i < strlen; i++) {
                if (position >= buffer.length - 3) {
                    // buffer is exhausted, as it cannot fit next three bytes,
                    // we need to flush it to the output stream
                    out.write(buffer, 0, position);
                    position = 0;
                }
                int c = cs.charAt(i);

                // manual loop unroll, as it performs much better on jdk8
                if (c < HIGH_BIT) {
                    buffer[position++] = (byte)c;
                } else if (c < HIGH_BIT14) {
                    buffer[position++] = (byte)(c | HIGH_BIT);
                    buffer[position++] = (byte)((c >>> 7));
                } else {
                    buffer[position++] = (byte)(c | HIGH_BIT);
                    buffer[position++] = (byte)((c >>> 7) | HIGH_BIT);
                    buffer[position++] = (byte)((c >>> 14));
                }
            }
            out.write(buffer, 0, position);

        } else {
            out.write(0);
        }
    }

    public static String readString(DataInput in) throws IOException {
        // the length we read is offset by one, because a length of zero indicates a null value
        int len = in.readUnsignedByte();

        if (len == 0) {
            return null;
        }

        if (len >= HIGH_BIT) {
            int shift = 7;
            int curr;
            len = len & 0x7f;
            while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
                len |= (curr & 0x7f) << shift;
                shift += 7;
            }
            len |= curr << shift;
        }

        // subtract one for the null length
        len -= 1;

        /* as we have no idea about byte-length of the serialized string, we cannot fully
         * read it into memory buffer. But we can do it in an optimistic way:
         * 1. In a happy case when the string is an us-ascii one, then byte_len == char_len, so we can read
         *    the whole byte sequence all at once.
         * 2. In case if there was at least one character with code >= 127, then we have some characters being not yet
         *    loaded into the buffer, so we refill the buffer.
         * 3. We also reuse the buffer to save on allocations.
         * 4. Buffer is also limited in size.
         *
         * So for an ascii string of size 33 (and STRING_BUFFER_SIZE = 16), we will read the following chunk sizes:
         * 16-16-1.
         *
         * For a string filled with 0xFFFF chars of size 33 (and STRING_BUFFER_SIZE = 16), effectively occupying 99 bytes,
         * we will read the following chunk sizes:
         * 16-16-16-16-12-8-5-4-2-2-1-1
         *
         * This long tail of non-16-byte chunks is happening because we never know will we spot a non-ascii character
         * later, or no. So we always refill the minimal buffer size.
         */

        // happily assume that the string is an 7 bit us-ascii one
        byte[] buf = stringByteBuffer.get();
        int bufSize = Math.min(len, buf.length);
        in.readFully(buf, 0, bufSize);

        char[] data;
        if (len < SHORT_STRING_MAX_LENGTH) {
            // skip allocating a separate buffer and reuse the thread-local one.
            // as allocating the buffer for small strings can produce too much GC pressure.
            data = shortStringCharBuffer.get();
        } else {
            data = new char[len];
        }

        int charPosition = 0;
        int bytePosition = 0;

        while (charPosition < len) {
            if (bytePosition == bufSize) {
                // there is at least `char count - char position` bytes left in case if all the
                // remaining characters are 7 bit.
                int minRemainingChars = len - charPosition;
                // need to refill the buffer as we already reached its end.
                // we also reuse the old buffer, as it's capacity must always be >= minRemainingChars.
                bufSize = Math.min(minRemainingChars, buf.length);
                in.readFully(buf, 0, bufSize);
                bytePosition = 0;
            }
            int c = buf[bytePosition++] & 255;
            // non 7-bit path
            if (c >= HIGH_BIT) {
                int shift = 7;
                int curr;
                c = c & 0x7f;
                if (bytePosition == bufSize) {
                    int minRemainingChars = len - charPosition;
                    bufSize = Math.min(minRemainingChars, buf.length);
                    in.readFully(buf, 0, bufSize);
                    bytePosition = 0;
                }

                while ((curr = buf[bytePosition++] & 255) >= HIGH_BIT) {
                    c |= (curr & 0x7f) << shift;
                    shift += 7;
                    if (bytePosition == bufSize) {
                        int minRemainingChars = len - charPosition;
                        // may need to refill the buffer if char bytes are split between the buffers.
                        bufSize = Math.min(minRemainingChars, buf.length);
                        in.readFully(buf, 0, bufSize);
                        bytePosition = 0;

                    }
                }
                c |= curr << shift;
            }
            data[charPosition++] = (char) c;
        }
        return new String(data, 0, len);
    }

}
