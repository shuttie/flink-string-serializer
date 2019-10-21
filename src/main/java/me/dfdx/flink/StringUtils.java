package me.dfdx.flink;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

public class StringUtils {

    private static final int HIGH_BIT = 0x1 << 7;

    private static final int VARLEN7 = 0x1 << 7;
    private static final int VARLEN14 = 0x1 << 14;
    private static final int VARLEN21 = 0x1 << 21;
    private static final int VARLEN28 = 0x1 << 28;

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
            while (lenToWrite >= HIGH_BIT) {
                buffer[position++] = (byte) (lenToWrite | HIGH_BIT);
                lenToWrite >>>= 7;
            }
            buffer[position++] = (byte) lenToWrite;

            // write the char data, variable length encoded
            for (int i = 0; i < strlen; i++) {
                char c = cs.charAt(i);

                while (c >= HIGH_BIT) {
                    buffer[position++] = (byte)(c | HIGH_BIT);
                    c >>>= 7;
                }
                buffer[position++] = (byte)(c | HIGH_BIT);
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

        final char[] data = new char[len];
        int utflen = 0;
        for (int i=0; i< len; i++) {
            int c = in.readUnsignedByte();
            if (c >= HIGH_BIT) {

            }
        }
        final byte[] buffer = new byte[len];

        for (int i = 0; i < len; i++) {
            int c = in.readUnsignedByte();
            if (c < HIGH_BIT) {
                data[i] = (char) c;
            } else {
                int shift = 7;
                int curr;
                c = c & 0x7f;
                while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
                    c |= (curr & 0x7f) << shift;
                    shift += 7;
                }
                c |= curr << shift;
                data[i] = (char) c;
            }
        }

        return new String(data, 0, len);
    }

}
