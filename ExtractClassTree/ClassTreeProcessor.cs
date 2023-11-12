using System.Diagnostics;
using System.Globalization;
using SharpCompress.Compressors;
using SharpCompress.Compressors.BZip2;

namespace ExtractClassTree;

public class ClassTreeProcessor : IDisposable, IAsyncDisposable
{
    private const int BufferSize = 0x10000;

    private const string EntityUriPrefix = "http://www.wikidata.org/entity/Q";
    private static readonly int EntityUriPrefixLength = EntityUriPrefix.Length;

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
    private long compressedPosition;
    private long compressedSize;

    private readonly object doneSync = new();

    private readonly string inputFilePath;

    private readonly Thread decompressionThread;
    private readonly Thread parsingThread;

    private readonly ClassTreeParser parser = new();

    private long currentQid = -1;
    private readonly BinaryWriter outputFile;

    private ulong superClassCount;

    public ClassTreeProcessor(string inputFilePath, string outputFilePath)
    {
        this.inputFilePath = inputFilePath;
        this.outputFile = new BinaryWriter(new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

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
            var lastWaitBuf = 0UL;
            var lastWaitData = 0UL;
            while (!Monitor.Wait(doneSync, 2500))
            {
                var currWaitBuf = Interlocked.Read(ref waitedForBuffer);
                var currWaitData = Interlocked.Read(ref waitedForData);
                var time = stopwatch.ElapsedMilliseconds;
                Console.WriteLine($"{time * 0.001f:F1} s: Decompressed {BytesToString((long) Volatile.Read(ref decompressedData))} ({BytesToString((long) Math.Round(decompressedData / (float) time * 1000.0f))}/s) from {BytesToString(compressedPosition)} ({BytesToString((long) Math.Round(compressedPosition / (float) time * 1000.0f))}/s), {Percentage(compressedPosition, compressedSize)}, ETA {ComputeEta(compressedPosition, compressedSize, time)}, found {superClassCount} superclasses (now at Q{currentQid}), waited buff={currWaitBuf - lastWaitBuf}, data={currWaitData - lastWaitData}");
                lastWaitBuf = currWaitBuf;
                lastWaitData = currWaitData;
            }
        }
        stopwatch.Stop();

        var totalTime = stopwatch.ElapsedMilliseconds;
        Console.WriteLine($"Done! Decompressed {BytesToString((long) Volatile.Read(ref decompressedData))} ({BytesToString((long) Math.Round(decompressedData / (float) totalTime * 1000.0f))}/s) from {BytesToString(compressedPosition)} ({BytesToString((long) Math.Round(compressedPosition / (float) totalTime * 1000.0f))}/s), found {superClassCount} superclasses (now at Q{currentQid}), duration {totalTime * 0.001f:F1} s, waited {waitedForBuffer} for buffer, {waitedForData} for data");
    }

    private static string Percentage(long curr, long total) => total == 0 ? "0 %" : $"{curr * 100.0f / total:F0} %";

    private static string ComputeEta(long curr, long total, long millis)
    {
        if (curr == 0)
        {
            return "?";
        }

        var timeSpan = TimeSpan.FromMilliseconds((total - curr) / (double) curr * millis);
        return timeSpan.ToString(timeSpan.Days > 0 ? "d'.'hh':'mm':'ss" : "h':'mm':'ss", CultureInfo.InvariantCulture);
    }

    private void DecompressionThreadProc()
    {
        using var stream = File.OpenRead(inputFilePath);
        compressedSize = stream.Length;
        using var decompressionStream = new BZip2Stream(stream, CompressionMode.Decompress, false);
        int bufferToWrite = 0;
        while (reachedEndOfStream == 0)
        {
            while (Volatile.Read(ref bufferFull[bufferToWrite]) != 0 || Volatile.Read(ref consumingBuffer[bufferToWrite]) != 0)
            {
                Interlocked.Increment(ref waitedForBuffer);
                Wait(100);
            }

            int readBytes = ReadFully(decompressionStream, buffers[bufferToWrite]);
            if (readBytes < BufferSize)
            {
                Volatile.Write(ref lastFilledBuffer, bufferToWrite);
                SwitchBoolFlag(ref reachedEndOfStream, false, true);
            }
            Interlocked.Add(ref decompressedData, (ulong) readBytes);
            compressedPosition = stream.Position;

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
                Wait(1000);
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
                var foundTriple = parser.ProcessCharacter(curr, buffer, i);
                if (foundTriple != null)
                {
                    var (subj, pred, obj) = foundTriple.GetValueOrDefault();
                    if (pred == "http://www.wikidata.org/prop/direct/P279")
                    {
                        var subjQid = GetQidFromEntityUri(subj);
                        var classQid = GetQidFromEntityUri(obj);
                        if (subjQid != currentQid)
                        {
                            if (currentQid >= 0)
                            {
                                outputFile.Write(0L);
                            }
                            outputFile.Write(subjQid);
                            currentQid = subjQid;
                        }
                        outputFile.Write(classQid);
                        Interlocked.Increment(ref superClassCount);
                    }
                }
                ++p;
            }
        }
        parser.ProcessEndOfBuffer(buffer);
    }

    private static long GetQidFromEntityUri(string uri) => Int64.Parse(uri.AsSpan(EntityUriPrefixLength), CultureInfo.InvariantCulture);

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

    private static void Wait(int count)
    {
        // Thread.Sleep(10);
        // Thread.Yield();
        Thread.SpinWait(count);
    }

    private static string BytesToString(long value)
    {
        string suffix;
        double readable;
        switch (Math.Abs(value))
        {
            case >= 0x1000000000000000:
                suffix = "EiB";
                readable = value >> 50;
                break;
            case >= 0x4000000000000:
                suffix = "PiB";
                readable = value >> 40;
                break;
            case >= 0x10000000000:
                suffix = "TiB";
                readable = value >> 30;
                break;
            case >= 0x40000000:
                suffix = "GiB";
                readable = value >> 20;
                break;
            case >= 0x100000:
                suffix = "MiB";
                readable = value >> 10;
                break;
            case >= 0x400:
                suffix = "KiB";
                readable = value;
                break;
            default:
                return value.ToString("0 B");
        }

        return (readable / 1024).ToString("0.## ", CultureInfo.InvariantCulture) + suffix;
    }

    public void Dispose()
    {
        outputFile.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await outputFile.DisposeAsync();
    }
}