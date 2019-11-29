package me.dfdx.flink;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringUtils {

    private static final int HIGH_BIT7 = 0x1 << 7;
    private static final int HIGH_BIT14 = 0x1 << 14;
    private static final int HIGH_BIT21 = 0x1 << 14;

    public static final void writeString(CharSequence cs, DataOutput out) throws IOException {
        if (cs != null) {
            // the length we write is offset by one, because a length of zero indicates a null value
            int position = 0;
            int strlen = cs.length();
            int buflen = 5; // worst-case when we have giant string with 5 bytes variable-length encoding
            for (int i = 0; i < strlen; i++) {
                char c = cs.charAt(i);
                if ((c >= 0x0001) && (c <= 0x007F)) {
                    buflen++;
                } else if (c > 0x07FF) {
                    buflen += 3;
                } else {
                    buflen += 2;
                }
            }
            byte[] buffer = new byte[buflen];
            int lenToWrite = strlen+1;
            if (lenToWrite < 0) {
                throw new IllegalArgumentException("CharSequence is too long.");
            }
            // write the length, variable-length encoded
            while (lenToWrite >= HIGH_BIT7) {
                buffer[position++] = (byte) (lenToWrite | HIGH_BIT7);
                lenToWrite >>>= 7;
            }
            buffer[position++] = (byte) lenToWrite;

            // write the char data, variable length encoded
            for (int i = 0; i < strlen; i++) {
                int c = cs.charAt(i);

                if (c < HIGH_BIT7) {
                    buffer[position++] = (byte)c;
                } else if (c < HIGH_BIT14) {
                    buffer[position++] = (byte)(c | HIGH_BIT7);
                    buffer[position++] = (byte)((c >>> 7));
                } else {
                    buffer[position++] = (byte)(c | HIGH_BIT7);
                    buffer[position++] = (byte)((c >>> 7) | HIGH_BIT7);
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

        if (len >= HIGH_BIT7) {
            int shift = 7;
            int curr;
            len = len & 0x7f;
            while ((curr = in.readUnsignedByte()) >= HIGH_BIT7) {
                len |= (curr & 0x7f) << shift;
                shift += 7;
            }
            len |= curr << shift;
        }

        // subtract one for the null length
        len -= 1;

        /* as we have no idea about byte-length of the serialized string, we cannot fully
         * read it into memory buffer. But we can do it in an optimistic way:
         * 1. In a happy case when the string is an us-ascii one, then byte_len == char_len
         * 2. If we spot at least one character with code >= 127, then we fall back to the old
         * unbuffered iterative approach.
         */

        // happily assume that the string is an 7 bit us-ascii one
        byte[] buf = new byte[len];
        in.readFully(buf);

        final char[] data = new char[len];
        int charPosition = 0;
        int bufSize = len;
        int bytePosition = 0;
        while (charPosition < len) {
            int remainingBytesEstimation = len - charPosition;
            int c;
            if (bytePosition == bufSize) {
                // need to expand the buffer
                //buf = new byte[remainingBytesEstimation];
                in.readFully(buf, 0, remainingBytesEstimation);
                bytePosition = 0;
                bufSize = remainingBytesEstimation;
            }
            c = buf[bytePosition++] & 255;
            if (c >= HIGH_BIT7) {
                int shift = 7;
                int curr;
                c = c & 0x7f;
                if (bytePosition == bufSize) {
                    // need to expand the buffer
                    //buf = new byte[remainingBytesEstimation];
                    in.readFully(buf, 0, remainingBytesEstimation);
                    bytePosition = 0;
                    bufSize = remainingBytesEstimation;
                }
                //int bytesRead = 1;
                while ((curr = buf[bytePosition++] & 255) >= HIGH_BIT7) {
                    //bytesRead++;
                    c |= (curr & 0x7f) << shift;
                    shift += 7;
                    if (bytePosition == bufSize) {
                        // need to expand the buffer
                        //buf = new byte[remainingBytesEstimation];
                        in.readFully(buf, 0, remainingBytesEstimation);
                        bytePosition = 0;
                        bufSize = remainingBytesEstimation;
                    }
                }
                c |= curr << shift;
            }
            data[charPosition++] = (char) c;
        }
        assert (bytePosition == buf.length);
        return new String(data, 0, len);
    }

}
