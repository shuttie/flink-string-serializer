package me.dfdx.flink;

import java.io.DataOutput;
import java.io.IOException;

public class StringSerializerImpl {

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
            int buflen = 4;
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

}
