using System.Diagnostics;
using SharpCompress.Compressors;
using SharpCompress.Compressors.BZip2;

namespace ExtractClassTree;

public class ClassTreeProcessor
{
    private const int BufferSize = 0x10000;

    private readonly byte[][] buffers =
    {
        new byte[BufferSize],
        new byte[BufferSize]
    };

    private int reachedEndOfStream;
    private int lastFilledBuffer = -1;

    private readonly int[] consumingBuffer = { 0, 0 };
    private readonly int[] bufferFull = { 0, 0 };

    private ulong waitedForData;
    private ulong waitedForBuffer;

    private ulong decompressedData;
    private ulong processedData;

    private ulong lineCount;

    private readonly object doneSync = new();

    private readonly string filePath;

    private readonly Thread decompressionThread;
    private readonly Thread parsingThread;

    public ClassTreeProcessor(string filePath)
    {
        this.filePath = filePath;

        decompressionThread = new Thread(DecompressionThreadProc)
        {
            Name = "DecompressionThread",
        };
        parsingThread = new Thread(ParsingThreadProc)
        {
            Name = "ParsingThread",
        };
    }

    public void Run()
    {
        var stopwatch = Stopwatch.StartNew();
        stopwatch.Start();

        Volatile.Write(ref decompressedData, 0UL);
        Volatile.Write(ref processedData, 0UL);

        decompressionThread.Start();
        parsingThread.Start();

        lock (doneSync)
        {
            while (!Monitor.Wait(doneSync, 2500))
            {
                Volatile.Read(ref bufferFull[0]);
                Volatile.Read(ref bufferFull[1]);
                var time = stopwatch.ElapsedMilliseconds;
                Console.WriteLine($"Decompressed {Volatile.Read(ref decompressedData)} bytes ({decompressedData / (float) time:F2} kB/s), processed {Interlocked.Read(ref processedData)} bytes ({processedData / (float) time:F2} kB/s), found {lineCount} lines, duration {time * 0.001f:F1} s, waited {waitedForBuffer} for buffer, {waitedForData} for data...");
            }
        }
        stopwatch.Stop();

        var totalTime = stopwatch.ElapsedMilliseconds;
        Console.WriteLine($"Done! Decompressed {decompressedData} bytes ({decompressedData / (float) totalTime:F2} kB/s), processed {processedData} bytes ({processedData / (float) totalTime:F2} kB/s), found {lineCount} lines, duration {totalTime * 0.001f:F1} s, waited {waitedForBuffer} for buffer, {waitedForData} for data...");
    }

    private void DecompressionThreadProc()
    {
        using var stream = File.OpenRead(filePath);
        using var decompressionStream = new BZip2Stream(stream, CompressionMode.Decompress, false);
        int bufferToWrite = 0;
        while (reachedEndOfStream == 0)
        {
            while (Volatile.Read(ref bufferFull[bufferToWrite]) != 0 || Volatile.Read(ref consumingBuffer[bufferToWrite]) != 0)
            {
                Interlocked.Increment(ref waitedForBuffer);
                // Thread.Sleep(10);
                // Thread.Yield();
                Thread.SpinWait(100);
            }

            int readBytes = ReadFully(decompressionStream, buffers[bufferToWrite]);
            if (readBytes < BufferSize)
            {
                Volatile.Write(ref lastFilledBuffer, bufferToWrite);
                SwitchBoolFlag(ref reachedEndOfStream, false, true);
            }
            Interlocked.Add(ref decompressedData, (ulong) readBytes);

            SwitchBoolFlag(ref bufferFull[bufferToWrite], false, true);
            bufferToWrite = 1 - bufferToWrite;
        }

        Console.WriteLine("Reading done");
    }

    void ParsingThreadProc()
    {
        var bufferToRead = 0;
        do
        {
            while (Volatile.Read(ref bufferFull[bufferToRead]) == 0)
            {
                Interlocked.Increment(ref waitedForData);
                // Thread.Sleep(10);
                // Thread.Yield();
                Thread.SpinWait(100);
            }

            SwitchBoolFlag(ref consumingBuffer[bufferToRead], false, true);
            SwitchBoolFlag(ref bufferFull[bufferToRead], true, false);

            // consume buffer[bufferToRead]
            ProcessData(buffers[bufferToRead]);

            // processedData += BufferSize;
            Interlocked.Add(ref processedData, BufferSize);

            SwitchBoolFlag(ref consumingBuffer[bufferToRead], true, false);
            bufferToRead = 1 - bufferToRead;
        } while (Volatile.Read(ref reachedEndOfStream) == 0 || lastFilledBuffer == bufferToRead);

        lock (doneSync)
        {
            Volatile.Write(ref lineCount, lineCount);
            Monitor.PulseAll(doneSync);
            Console.WriteLine("Processing done");
        }
    }

    static int ReadFully(Stream stream, byte[] buffer)
    {
        int readTotal = 0;
        int read;
        var length = buffer.Length;
        while (readTotal < length && (read = stream.Read(buffer, readTotal, length - readTotal)) > 0)
        {
            readTotal += read;
        }
        if (readTotal < length)
        {
            Array.Fill(buffer, (byte) 0, readTotal, length - readTotal);
        }
        return readTotal;
    }

    unsafe void ProcessData(byte[] buffer)
    {
        var len = buffer.Length;
        fixed (byte* bufPointer = buffer)
        {
            var p = bufPointer;
            for (int i = 0; i < len; ++i)
            {
                var curr = *p;
                if (curr == 0)
                {
                    // end of buffer
                    break;
                }
                if (curr == '\n')
                {
                    ++lineCount;
                }
                ++p;
            }
        }
    }

    private static void SwitchBoolFlag(ref int flag, bool from, bool to)
    {
        var fromVal = from ? 1 : 0;
        var toVal = to ? 1 : 0;
        if (Interlocked.CompareExchange(ref flag, toVal, fromVal) != fromVal)
        {
            throw new InvalidOperationException("Async state mismatch!");
        }
        // Debug.Assert(Volatile.Read(ref flag) == toVal);
    }
}