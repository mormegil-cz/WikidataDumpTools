using System.Diagnostics;
using SharpCompress.Compressors;
using SharpCompress.Compressors.BZip2;

const int BufferSize = 0x1000;

var buffers = new[]
{
    new byte[BufferSize],
    new byte[BufferSize]
};
bool reachedEndOfStream = false;
int lastFilledBuffer = -1;

var consumingBuffer = new[] { false, false };
var bufferFull = new[] { false, false };

ulong waitedForData = 0UL;
ulong waitedForBuffer = 0UL;

ulong decompressedData = 0UL;
ulong processedData = 0UL;

var doneSync = new object();

var decompressionThread = new Thread(DecompressionThreadProc)
{
    Name = "DecompressionThread",
};
var parsingThread = new Thread(ParsingThreadProc)
{
    Name = "ParsingThread",
};

var stopwatch = Stopwatch.StartNew();
stopwatch.Start();
decompressionThread.Start();
parsingThread.Start();

lock (doneSync)
{
    while (!Monitor.Wait(doneSync, 1000))
    {
        var time = stopwatch.ElapsedMilliseconds;
        Console.WriteLine($"Decompressed {decompressedData} bytes ({decompressedData / (float) time:F2} kB/s), processed {processedData} bytes ({processedData / (float) time:F2} kB/s), duration {time * 0.001f:F1} s, waited {waitedForBuffer} for buffer, {waitedForData} for data...");
    }
}
stopwatch.Stop();

Console.WriteLine($"Done! Decompressed {decompressedData} bytes, processed {processedData} bytes, duration {stopwatch.Elapsed}, waited {waitedForBuffer} for buffer, {waitedForData} for data");

void DecompressionThreadProc()
{
    using var stream = File.OpenRead(@"/media/petr/Bigfoot/Wikidata-Dump/2023-08-18-truthy.nt.bz2");
    using var decompressionStream = new BZip2Stream(stream, CompressionMode.Decompress, false);
    int bufferToWrite = 0;
    while (!reachedEndOfStream)
    {
        while (Volatile.Read(ref bufferFull[bufferToWrite]) || Volatile.Read(ref consumingBuffer[bufferToWrite]))
        {
            Interlocked.Increment(ref waitedForBuffer);
            Thread.Yield();
            // Thread.SpinWait(10);
        }

        int readBytes = ReadFully(stream, buffers[bufferToWrite]);
        if (readBytes < BufferSize)
        {
            Volatile.Write(ref lastFilledBuffer, bufferToWrite);
            Volatile.Write(ref reachedEndOfStream, true);
        }
        decompressedData += (ulong) readBytes;

        Volatile.Write(ref bufferFull[bufferToWrite], true);
        bufferToWrite = 1 - bufferToWrite;
    }

    Console.WriteLine("Reading done");
}

void ParsingThreadProc()
{
    var bufferToRead = 0;
    do
    {
        while (!Volatile.Read(ref bufferFull[bufferToRead]))
        {
            Interlocked.Increment(ref waitedForData);
            // Thread.Sleep(100);
            Thread.Yield();
            // Thread.SpinWait(100);
        }

        Volatile.Write(ref consumingBuffer[bufferToRead], true);
        Volatile.Write(ref bufferFull[bufferToRead], false);

        // consume buffer[bufferToRead]
        processedData += BufferSize;

        Volatile.Write(ref consumingBuffer[bufferToRead], false);
        bufferToRead = 1 - bufferToRead;
    } while (!Volatile.Read(ref reachedEndOfStream) || lastFilledBuffer == bufferToRead);

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