using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.Marshalling;
using System.Text;

namespace ExtractClassTree;

public class ClassTreeParser
{
    private enum State
    {
        Init,
        SkipRestOfLine,
        InSubject,
        AfterSubject,
        InPredicate,
        AfterPredicate,
        InObject,
    }

    private State state = State.Init;

    private byte[]? subjectPrefix;
    private byte[]? predicatePrefix;
    private byte[]? objectPrefix;

    private string? subjectUri;
    private string? predicateUri;
    private string? objectUri;

    private int subjectStart;
    private int predicateStart;
    private int objectStart;

    private int errors;

    public (string, string, string)? ProcessCharacter(byte b, byte[] buffer, int index)
    {
        var c = (char) b;
        //Console.WriteLine($"Character '{c}' at {index}, state {state} ({subjectStart}, {predicateStart}, {objectStart})");
        switch (state)
        {
            case State.Init:
                switch (c)
                {
                    case '<':
                        subjectStart = index;
                        state = State.InSubject;
                        break;
                    case '\n':
                        // nothing, empty line
                        break;
                    default:
                        state = State.SkipRestOfLine;
                        break;
                }
                break;

            case State.InSubject:
                switch (c)
                {
                    case '>':
                        subjectUri = ExtractString(subjectPrefix, buffer, subjectStart, index);
                        subjectStart = -1;
                        subjectPrefix = null;
                        state = State.AfterSubject;
                        break;
                    case '\n':
                        // duh! line inside subject?
                        ++errors;
                        subjectStart = -1;
                        subjectPrefix = null;
                        state = State.Init;
                        break;
                }
                break;

            case State.AfterSubject:
                switch (c)
                {
                    case '<':
                        predicateStart = index;
                        state = State.InPredicate;
                        break;
                    case '\n':
                        // duh! linebreak after subject?
                        ++errors;
                        state = State.Init;
                        break;
                    default:
                        if (Char.IsWhiteSpace(c))
                        {
                            // OK, skipping whitespace
                        }
                        else
                        {
                            // duh! something else than predicate?
                            ++errors;
                            state = State.SkipRestOfLine;
                        }
                        break;
                }
                break;

            case State.InPredicate:
                switch (c)
                {
                    case '>':
                        predicateUri = ExtractString(predicatePrefix, buffer, predicateStart, index);
                        predicateStart = -1;
                        predicatePrefix = null;
                        state = State.AfterPredicate;
                        break;
                    case '\n':
                        // duh! line inside predicate?
                        ++errors;
                        predicateStart = -1;
                        predicatePrefix = null;
                        state = State.Init;
                        break;
                }
                break;

            case State.AfterPredicate:
                switch (c)
                {
                    case '<':
                        objectStart = index;
                        state = State.InObject;
                        break;
                    case '\n':
                        // duh! linebreak after predicate?
                        ++errors;
                        state = State.Init;
                        break;
                    default:
                        if (Char.IsWhiteSpace(c))
                        {
                            // OK, skipping whitespace
                        }
                        else
                        {
                            // something else (non-item value), ignoring the whole line (but not an error)
                            state = State.SkipRestOfLine;
                        }
                        break;
                }
                break;

            case State.InObject:
                switch (c)
                {
                    case '>':
                        objectUri = ExtractString(objectPrefix, buffer, objectStart, index);
                        objectStart = -1;
                        objectPrefix = null;
                        state = State.SkipRestOfLine;
                        // predicate completed!
                        return (subjectUri!, predicateUri!, objectUri!);
                    case '\n':
                        // duh! line inside object?
                        ++errors;
                        objectStart = -1;
                        objectPrefix = null;
                        state = State.Init;
                        break;
                }
                break;

            case State.SkipRestOfLine:
                if (c == '\n')
                {
                    state = State.Init;
                    subjectUri = null;
                    predicateUri = null;
                    objectUri = null;
                }
                break;

            default:
                throw new InvalidOperationException("Unexpected state!");
        }

        return null;
    }

    public void ProcessEndOfBuffer(byte[] buffer)
    {
        //Console.WriteLine($"End of buffer in state {state} ({subjectStart}, {predicateStart}, {objectStart})");
        switch (state)
        {
            case State.InSubject:
                subjectPrefix = ExtractPrefix(buffer, subjectStart);
                subjectStart = 0;
                break;

            case State.InPredicate:
                predicatePrefix = ExtractPrefix(buffer, predicateStart);
                predicateStart = 0;
                break;

            case State.InObject:
                objectPrefix = ExtractPrefix(buffer, objectStart);
                objectStart = 0;
                break;

            default:
                // no processing needed for the rest of the states
                break;
        }
    }

    private byte[] ExtractPrefix(byte[] buffer, int start)
    {
        var prefixLength = buffer.Length - start - 1;
        var result = new byte[prefixLength];
        Array.Copy(buffer, start + 1, result, 0, prefixLength);
        return result;
    }

    private static string ExtractString(byte[]? prefix, byte[] buffer, int start, int end)
    {
        if (prefix == null)
        {
            return Encoding.UTF8.GetString(buffer, start + 1, end - start - 1);
        }

        Debug.Assert(start == 0);
        if (end == 0)
        {
            return Encoding.UTF8.GetString(prefix);
        }
        
        var prefixLength = prefix.Length;
        var currBuffLen = end - 1;
        var length = prefixLength + currBuffLen;
        byte[] fullBuff = new byte[length];
        Array.Copy(prefix, fullBuff, prefixLength);
        Array.Copy(buffer, 0, fullBuff, prefixLength, currBuffLen);

        return Encoding.UTF8.GetString(fullBuff);
    }

    private static unsafe string ExtractStringUnsafe(byte[]? prefix, byte[] buffer, int start, int end)
    {
        if (prefix == null)
        {
            return Encoding.UTF8.GetString(buffer, start + 1, end - start - 1);
        }

        var prefixLength = prefix.Length;
        var currBuffLen = end - start - 1;
        var length = prefixLength + currBuffLen;
        byte* fullBuff = stackalloc byte[length];
        Unsafe.CopyBlock(ref *fullBuff, ref prefix[0], (uint) prefixLength);
        Unsafe.CopyBlock(ref *(fullBuff + prefixLength), ref buffer[start + 1], (uint) currBuffLen);

        return Encoding.UTF8.GetString(fullBuff, length);
    }
}