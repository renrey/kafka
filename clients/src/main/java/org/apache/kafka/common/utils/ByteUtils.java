/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * This classes exposes low-level methods for reading/writing from byte streams or buffers.
 */
public final class ByteUtils {

    public static final ByteBuffer EMPTY_BUF = ByteBuffer.wrap(new byte[0]);

    private ByteUtils() {}

    /**
     * Read an unsigned integer from the current position in the buffer, incrementing the position by 4 bytes
     *
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer) {
        return buffer.getInt() & 0xffffffffL;
    }

    /**
     * Read an unsigned integer from the given position without modifying the buffers position
     *
     * @param buffer the buffer to read from
     * @param index the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer, int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }

    /**
     * Read an unsigned integer stored in little-endian format from the {@link InputStream}.
     *
     * @param in The stream to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    public static int readUnsignedIntLE(InputStream in) throws IOException {
        return in.read()
                | (in.read() << 8)
                | (in.read() << 16)
                | (in.read() << 24);
    }

    /**
     * Read an unsigned integer stored in little-endian format from a byte array
     * at a given offset.
     *
     * @param buffer The byte array to read from
     * @param offset The position in buffer to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    public static int readUnsignedIntLE(byte[] buffer, int offset) {
        return (buffer[offset] << 0 & 0xff)
                | ((buffer[offset + 1] & 0xff) << 8)
                | ((buffer[offset + 2] & 0xff) << 16)
                | ((buffer[offset + 3] & 0xff) << 24);
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param index The position in the buffer at which to begin writing
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, int index, long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, long value) {
        buffer.putInt((int) (value & 0xffffffffL));
    }

    /**
     * Write an unsigned integer in little-endian format to the {@link OutputStream}.
     *
     * @param out The stream to write to
     * @param value The value to write
     */
    public static void writeUnsignedIntLE(OutputStream out, int value) throws IOException {
        out.write(value);
        out.write(value >>> 8);
        out.write(value >>> 16);
        out.write(value >>> 24);
    }

    /**
     * Write an unsigned integer in little-endian format to a byte array
     * at a given offset.
     *
     * @param buffer The byte array to write to
     * @param offset The position in buffer to write to
     * @param value The value to write
     */
    public static void writeUnsignedIntLE(byte[] buffer, int offset, int value) {
        buffer[offset] = (byte) value;
        buffer[offset + 1] = (byte) (value >>> 8);
        buffer[offset + 2] = (byte) (value >>> 16);
        buffer[offset + 3]   = (byte) (value >>> 24);
    }

    /**
     * Read an integer stored in variable-length format using unsigned decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     */
    public static int readUnsignedVarint(ByteBuffer buffer) {
        int value = 0;
        int i = 0;
        int b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 28)
                throw illegalVarintException(value);
        }
        value |= b << i;
        return value;
    }

    /**
     * Read an integer stored in variable-length format using unsigned decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param in The input to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     * @throws IOException              if {@link DataInput} throws {@link IOException}
     */
    public static int readUnsignedVarint(DataInput in) throws IOException {
        int value = 0;
        int i = 0;
        int b;
        while (((b = in.readByte()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 28)
                throw illegalVarintException(value);
        }
        value |= b << i;
        return value;
    }

    /**
     * Read an integer stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     */
    public static int readVarint(ByteBuffer buffer) {
        int value = readUnsignedVarint(buffer);
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read an integer stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param in The input to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     * @throws IOException              if {@link DataInput} throws {@link IOException}
     */
    public static int readVarint(DataInput in) throws IOException {
        int value = readUnsignedVarint(in);
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read a long stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param in The input to read from
     * @return The long value read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes have been read
     * @throws IOException              if {@link DataInput} throws {@link IOException}
     */
    public static long readVarlong(DataInput in) throws IOException {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = in.readByte()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 63)
                throw illegalVarlongException(value);
        }
        value |= b << i;
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read a long stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The long value read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes have been read
     */
    public static long readVarlong(ByteBuffer buffer)  {
        long value = 0L;
        int i = 0;
        long b;
        // 获取字节8位（第一位不是0进入）
        while (((b = buffer.get()) & 0x80) != 0) {
            // 不是全部等于0
            // 后7位1 & 8位字节b：0XXXXX ，后7位不变，第一个0 （因为还原到的value，原先都是0，只要能保持0就可以）
            // << i ：移动当数字对应的位置
            // value| = 累加value，因为0|X=X，等于对应位置已还原回对应数字
            value |= (b & 0x7f) << i;
            // 位置+7
            i += 7;
            if (i > 63)
                throw illegalVarlongException(value);
        }
        // 把最后一个字节（第一位是0）也还原到value，也就是已经发送前的值，varint还原完成
        value |= b << i;
        //zig-zag解码
        // value >>> 1 : 移除末尾的符号位，且把31位的数字移动回后31位
        // value & 00..01: 保留末尾符号的值，其他全为0 ， 即00...1(负数)或者000..0（正数）
        // -（value &1 ）：取反+1 ，所以负数：1111..111 , 正数00..00
        // ^ : 正数(全部0)：不变，还是value >>> 1的值
        // 负数（全部1）：全部取反，即1XXXX...X
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read a double-precision 64-bit format IEEE 754 value.
     *
     * @param in The input to read from
     * @return The double value read
     */
    public static double readDouble(DataInput in) throws IOException {
        return in.readDouble();
    }

    /**
     * Read a double-precision 64-bit format IEEE 754 value.
     *
     * @param buffer The buffer to read from
     * @return The long value read
     */
    public static double readDouble(ByteBuffer buffer) {
        return buffer.getDouble();
    }

    /**
     * Write the given integer following the variable-length unsigned encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The output to write to
     */
    public static void writeUnsignedVarint(int value, ByteBuffer buffer) {
        while ((value & 0xffffff80) != 0L) {
            byte b = (byte) ((value & 0x7f) | 0x80);
            buffer.put(b);
            value >>>= 7;
        }
        buffer.put((byte) value);
    }

    /**
     * Write the given integer following the variable-length unsigned encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * 变长整型varint：
     *  作用：
     *  就是可以用可变数量的byte字节来表示一个int，（可能1个byte、可能3个），减少传输的字节
     *  原因：
     *  因为一个比较小的正数，正常32位前面基本都是0，这样可以压缩前面重复的0，所以数字越小需要的byte越小，
     *  使用：
     *  每个字节的第一位为msb用来判断当前字节是否用来与下一个字节表示一个数，1代表一起同一个数，0的话，就代表当前字节就是这个数最后的一个，下一个字节就是用来表示这个数
     *  每个字节的后7位就是原始数据
     *  varint字节的顺序其实是小端字节序（原始值的字节倒序），就是其实是把原始值的后面byte放到了前面，因此解析varint需要翻转
     *  缺点：
     *  只能对正数有优化，负数第一位就是1，不可能压缩
     *
     *
     * @param value The value to write
     * @param out The output to write to
     *
     */
    public static void writeUnsignedVarint(int value, DataOutput out) throws IOException {
        // 数值（绝对值）不等于0
        while ((value & 0xffffff80) != 0L) {
            // 1. value & 0111 1111（7位1）：value后7位不变，就拿到value的后7位，前面都是0, 0XXX XXXX
            // 2. 0XXX XXXX | 1000 0000 : 1XXX XXXX ，第一位1，后7位是value后7位
            byte b = (byte) ((value & 0x7f) | 0x80);
            out.writeByte(b);
            // 右移7位，把已经写入的7位去掉，下次又写入7位
            value >>>= 7;
        }
        // 最后一定会写入0的字节（0000 0000），代表这个数终止
        out.writeByte((byte) value);
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the output.
     * 使用varint 和 zig-zag 对value进行编码压缩
     *
     * zig-zag：对负数（第一位1，剩下的是绝对值的反码再补码（+1））优化，使得负数变成正数的形式（首位不等于1），可以进行varint的优化
     *          并且对正数处理不会有变化
     * 因此先对value进行zig-zag处理，再进行
     * @param value The value to write
     * @param out The output to write to
     */
    public static void writeVarint(int value, DataOutput out) throws IOException {
        // (value << 1) ^ (value >> 31)就是zig-zag的处理
        /**
         * (value << 1) ^ (value >> 31)：
         * 正数: XXXX...0 ^ 0000...0(高位补0) = 后31位放到了前31位，最后一位0 基本没变
         * 负数：XXXX...0 ^ 1111...1（高位补1）= 后31放到前31位，并取反，最后一位1
         * 实际得到数就是把符号位放在了后面，原后31位放在前面31位（就是值）
         * 而负数会把那31位的值进行取反，正数则没变化
         */
        writeUnsignedVarint((value << 1) ^ (value >> 31), out);
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The output to write to
     */
    public static void writeVarint(int value, ByteBuffer buffer) {
        writeUnsignedVarint((value << 1) ^ (value >> 31), buffer);
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the output.
     * 不只是使用varint，还是用zig-zag进行编码
     * zig-zag：主要为了负数时的压缩，因为负数第一位就是1
     *
     * @param value The value to write
     * @param out The output to write to
     */
    public static void writeVarlong(long value, DataOutput out) throws IOException {
        // xxxx .. xxx0 ^ 0000.... 0000x : 把第1位移到最后一位，
        // zig-zag (符号位移到末尾，负数的数字位取反)
        long v = (value << 1) ^ (value >> 63);
        // v 不等于0
        while ((v & 0xffffffffffffff80L) != 0L) {
            // 1. value & 0111 1111（7位1）：value后7位不变，就拿到value的后7位，前面都是0, 0XXX XXXX
            // 2. 0XXX XXXX | 1000 0000 : 1XXX XXXX ，第一位1，后7位是value后7位
            out.writeByte(((int) v & 0x7f) | 0x80);
            v >>>= 7;
        }
        out.writeByte((byte) v);
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The buffer to write to
     */
    public static void writeVarlong(long value, ByteBuffer buffer) {
        long v = (value << 1) ^ (value >> 63);
        while ((v & 0xffffffffffffff80L) != 0L) {
            byte b = (byte) ((v & 0x7f) | 0x80);
            buffer.put(b);
            v >>>= 7;
        }
        buffer.put((byte) v);
    }

    /**
     * Write the given double following the double-precision 64-bit format IEEE 754 value into the output.
     *
     * @param value The value to write
     * @param out The output to write to
     */
    public static void writeDouble(double value, DataOutput out) throws IOException {
        out.writeDouble(value);
    }

    /**
     * Write the given double following the double-precision 64-bit format IEEE 754 value into the buffer.
     *
     * @param value The value to write
     * @param buffer The buffer to write to
     */
    public static void writeDouble(double value, ByteBuffer buffer) {
        buffer.putDouble(value);
    }

    /**
     * Number of bytes needed to encode an integer in unsigned variable-length format.
     *
     * @param value The signed value
     */
    public static int sizeOfUnsignedVarint(int value) {
        int bytes = 1;
        while ((value & 0xffffff80) != 0L) {
            bytes += 1;
            value >>>= 7;
        }
        return bytes;
    }

    /**
     * Number of bytes needed to encode an integer in variable-length format.
     *
     * @param value The signed value
     */
    public static int sizeOfVarint(int value) {
        return sizeOfUnsignedVarint((value << 1) ^ (value >> 31));
    }

    /**
     * Number of bytes needed to encode a long in variable-length format.
     *
     * @param value The signed value
     */
    public static int sizeOfVarlong(long value) {
        long v = (value << 1) ^ (value >> 63);
        int bytes = 1;
        while ((v & 0xffffffffffffff80L) != 0L) {
            bytes += 1;
            v >>>= 7;
        }
        return bytes;
    }

    private static IllegalArgumentException illegalVarintException(int value) {
        throw new IllegalArgumentException("Varint is too long, the most significant bit in the 5th byte is set, " +
                "converted value: " + Integer.toHexString(value));
    }

    private static IllegalArgumentException illegalVarlongException(long value) {
        throw new IllegalArgumentException("Varlong is too long, most significant bit in the 10th byte is set, " +
                "converted value: " + Long.toHexString(value));
    }
}
