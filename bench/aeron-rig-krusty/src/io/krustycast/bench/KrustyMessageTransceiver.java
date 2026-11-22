/*
 * Copyright 2025-2026 Dan Mihai Dumitriu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.krustycast.bench;

import io.aeron.benchmarks.Configuration;
import io.aeron.benchmarks.MessageTransceiver;
import org.HdrHistogram.ValueRecorder;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.DatagramChannel;

/**
 * KrustyCast for aeron-io/benchmarks' LoadTestRig (docs/BASELINE.md, E3):
 * the rig that measures Aeron Cluster, driving a KrustyCast cluster instead.
 * A message's round trip ends at its ACCEPTED, as Aeron's ends at its echo.
 * <p>
 * Two threads, as an Aeron client with its media driver: the rig's thread
 * (send, receive) runs the session (seqnos, window, resends, NOT_LEADER) and
 * hands SUBMITs to the "krusty-io" thread through a one-to-one ring, as an
 * Aeron publication hands frames to the driver's sender. The I/O thread
 * packs what is ready back to back into datagrams of up to krusty.mtu
 * bytes, with no waiting (ARCHITECTURE §8.1), and hands each datagram it
 * receives (one or more outcomes) back through a second ring. bench/aeron-rig-krusty/run.sh
 * pins the two threads as bench/aeron-cluster/run.sh pins Aeron's client.
 * <p>
 * Properties: krusty.nodes (ingress addresses in node id order, comma
 * separated), krusty.client.id (1), krusty.window (1024, a power of two, at
 * most the nodes' outcome_ring), krusty.mtu (1408, Aeron's default),
 * krusty.retry.ms (50).
 */
public final class KrustyMessageTransceiver extends MessageTransceiver
{
    // krusty-wire (crates/krusty-wire/src/lib.rs, ingress.rs, feedback.rs).
    private static final short MAGIC = 0x434B;
    private static final byte VERSION = 1;
    private static final byte SUBMIT = 0x01;
    private static final byte ACCEPTED = 0x10;
    private static final byte NAK = 0x12;
    private static final byte BUSY = 0x13;
    private static final byte NOT_LEADER = 0x14;
    private static final byte SESSION_EXPIRED = 0x15;
    private static final int HEADER_LEN = 48;
    private static final int FEEDBACK_LEN = 48;

    private InetSocketAddress[] nodes;
    private volatile int target;
    private int clientId;
    private long incarnation;
    private int window;
    private int mask;
    private long retryNs;
    private int mtu;

    // Session (the rig's thread).
    private long next;      // last seqno assigned
    private long oldest = 1; // lowest unanswered seqno
    private long[] timestamps;
    private long[] sentAt;
    private boolean[] done;
    private long checksum;
    private int messageLength;
    private final UnsafeBuffer scratch = new UnsafeBuffer(new byte[HEADER_LEN + 65536]);
    private long resends;

    private OneToOneRingBuffer toIo;
    private OneToOneRingBuffer fromIo;
    private DatagramChannel channel;
    private Thread io;
    private volatile boolean running = true;
    // I/O thread counters (read after it stops).
    private long datagramsSent;
    private long submitsSent;
    private long sendDrops;
    private long datagramsReceived;
    private long receiveDrops;

    public KrustyMessageTransceiver(final NanoClock clock, final ValueRecorder valueRecorder)
    {
        super(clock, valueRecorder);
    }

    private static OneToOneRingBuffer ring(final int capacity)
    {
        return new OneToOneRingBuffer(new UnsafeBuffer(
            ByteBuffer.allocateDirect(capacity + RingBufferDescriptor.TRAILER_LENGTH)));
    }

    public void init(final Configuration configuration) throws Exception
    {
        final String[] list = System.getProperty("krusty.nodes", "127.0.0.1:27001").split(",");
        nodes = new InetSocketAddress[list.length];
        for (int i = 0; i < list.length; i++)
        {
            final String[] hp = list[i].trim().split(":");
            nodes[i] = new InetSocketAddress(hp[0], Integer.parseInt(hp[1]));
        }
        clientId = Integer.getInteger("krusty.client.id", 1);
        window = Integer.getInteger("krusty.window", 1024);
        if (Integer.bitCount(window) != 1)
        {
            throw new IllegalArgumentException("krusty.window must be a power of two");
        }
        mask = window - 1;
        mtu = Integer.getInteger("krusty.mtu", 1408);
        retryNs = Long.getLong("krusty.retry.ms", 50) * 1_000_000L;
        incarnation = System.currentTimeMillis() * 1_000_000L;
        timestamps = new long[window];
        sentAt = new long[window];
        done = new boolean[window];
        messageLength = configuration.messageLength();
        toIo = ring(1 << 22);
        fromIo = ring(1 << 22);
        channel = DatagramChannel.open();
        channel.bind(new InetSocketAddress("127.0.0.1", 0));
        channel.configureBlocking(false);
        channel.setOption(StandardSocketOptions.SO_RCVBUF, 8 << 20);
        channel.setOption(StandardSocketOptions.SO_SNDBUF, 2 << 20);
        io = new Thread(this::ioLoop, "krusty-io");
        io.setDaemon(true);
        io.start();
    }

    public void destroy() throws Exception
    {
        running = false;
        io.join();
        channel.close();
        System.out.printf(
            "krusty: %d SUBMITs in %d datagrams (%.2f per datagram), %d resent, %d send drops; " +
            "%d datagrams of outcomes received, %d dropped%n",
            submitsSent, datagramsSent, datagramsSent == 0 ? 0.0 : (double)submitsSent / datagramsSent,
            resends, sendDrops, datagramsReceived, receiveDrops);
    }

    // ------------------------------------------------------------------
    // The rig's thread: the session.

    /** Encodes SUBMIT `seq` and hands it to the I/O thread. */
    private boolean enqueue(final long seq)
    {
        final int slot = (int)(seq & mask);
        final UnsafeBuffer b = scratch;
        final int len = messageLength;
        b.setMemory(0, HEADER_LEN, (byte)0);
        b.putShort(0, MAGIC, ByteOrder.LITTLE_ENDIAN);
        b.putByte(2, VERSION);
        b.putByte(3, SUBMIT);
        b.putInt(8, clientId, ByteOrder.LITTLE_ENDIAN);
        b.putShort(12, (short)len, ByteOrder.LITTLE_ENDIAN);
        b.putLong(16, incarnation, ByteOrder.LITTLE_ENDIAN);
        b.putLong(24, seq, ByteOrder.LITTLE_ENDIAN);
        b.setMemory(HEADER_LEN, len, (byte)0);
        b.putLong(HEADER_LEN, timestamps[slot], ByteOrder.LITTLE_ENDIAN);
        b.putLong(HEADER_LEN + len - Long.BYTES, checksum, ByteOrder.LITTLE_ENDIAN);
        if (!toIo.write(1, b, 0, HEADER_LEN + len))
        {
            return false;
        }
        sentAt[slot] = System.nanoTime();
        return true;
    }

    public int send(final int numberOfMessages, final int messageLength, final long timestamp, final long checksum)
    {
        this.checksum = checksum;
        int count = 0;
        while (count < numberOfMessages && next - oldest + 1 < window)
        {
            final long seq = next + 1;
            final int slot = (int)(seq & mask);
            timestamps[slot] = timestamp;
            done[slot] = false;
            if (!enqueue(seq))
            {
                break;
            }
            next = seq;
            count++;
        }
        return count;
    }

    /** Go back N: resends every unanswered message from `from` on. */
    private void resendFrom(final long from)
    {
        for (long s = Math.max(from, oldest); s <= next; s++)
        {
            if (!done[(int)(s & mask)])
            {
                if (!enqueue(s))
                {
                    return; // the timer tries again
                }
                resends++;
            }
        }
    }

    private void onDatagram(final int msgTypeId, final org.agrona.MutableDirectBuffer buffer, final int index, final int length)
    {
        for (int at = index; at + FEEDBACK_LEN <= index + length; at += FEEDBACK_LEN)
        {
            if (buffer.getShort(at, ByteOrder.LITTLE_ENDIAN) != MAGIC ||
                buffer.getInt(at + 8, ByteOrder.LITTLE_ENDIAN) != clientId ||
                buffer.getLong(at + 16, ByteOrder.LITTLE_ENDIAN) != incarnation)
            {
                continue;
            }
            final byte type = buffer.getByte(at + 3);
            final long seq = buffer.getLong(at + 24, ByteOrder.LITTLE_ENDIAN);
            final long a = buffer.getLong(at + 32, ByteOrder.LITTLE_ENDIAN);
            switch (type)
            {
                case ACCEPTED:
                    if (seq >= oldest && seq <= next && !done[(int)(seq & mask)])
                    {
                        done[(int)(seq & mask)] = true;
                        onMessageReceived(timestamps[(int)(seq & mask)], checksum);
                        while (oldest <= next && done[(int)(oldest & mask)])
                        {
                            oldest++;
                        }
                    }
                    break;
                case NAK:
                    resendFrom(seq); // seq: the one the cluster expects
                    break;
                case NOT_LEADER:
                    if (a >= 1 && a <= nodes.length && a - 1 != target)
                    {
                        target = (int)(a - 1);
                        resendFrom(oldest);
                    }
                    break;
                case BUSY:
                    break; // the retry timer resends
                case SESSION_EXPIRED:
                    throw new IllegalStateException("krusty: SESSION_EXPIRED for seqno " + seq);
                default:
                    throw new IllegalStateException("krusty: outcome type " + type + " for seqno " + seq);
            }
        }
    }

    public void receive()
    {
        fromIo.read(this::onDatagram, 64);
        if (oldest <= next && System.nanoTime() - sentAt[(int)(oldest & mask)] > retryNs)
        {
            resendFrom(oldest);
        }
    }

    // ------------------------------------------------------------------
    // The I/O thread: the counterpart of the media driver.

    private final ByteBuffer out = ByteBuffer.allocateDirect(65536);
    private final ByteBuffer in = ByteBuffer.allocateDirect(65536);
    private final UnsafeBuffer inView = new UnsafeBuffer(in);

    private void flush()
    {
        if (out.position() == 0)
        {
            return;
        }
        out.flip();
        try
        {
            if (channel.send(out, nodes[target]) == 0)
            {
                sendDrops++;
            }
        }
        catch (final java.io.IOException e)
        {
            sendDrops++;
        }
        datagramsSent++;
        out.clear();
    }

    private void onSubmit(final int msgTypeId, final org.agrona.MutableDirectBuffer buffer, final int index, final int length)
    {
        if (out.position() + length > mtu)
        {
            flush();
        }
        buffer.getBytes(index, out, out.position(), length);
        out.position(out.position() + length);
        submitsSent++;
    }

    private void ioLoop()
    {
        while (running)
        {
            toIo.read(this::onSubmit, 1024);
            flush();
            while (true)
            {
                in.clear();
                final java.net.SocketAddress from;
                try
                {
                    from = channel.receive(in);
                }
                catch (final java.io.IOException e)
                {
                    break;
                }
                if (from == null)
                {
                    break;
                }
                datagramsReceived++;
                final int n = in.position();
                int tries = 0;
                while (!fromIo.write(2, inView, 0, n) && ++tries < 1000)
                {
                    Thread.onSpinWait();
                }
                if (tries == 1000)
                {
                    receiveDrops++; // the session's timer resends
                }
            }
        }
    }
}
