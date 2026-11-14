using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using DiscUtils.Streams;
using DiscUtils.Vhdx;

class VhdxReference
{
    static int Main(string[] args)
    {
        var disks = new List<DiskImageFile>();
        var streams = new List<SparseStream>();
        try
        {
            if (args.Length == 0)
                throw new ArgumentException("Usage: vhdx_reference.exe root.vhdx [child.vhdx ...]");
            SparseStream view = null;
            foreach (string path in args)
            {
                var disk = new DiskImageFile(path, FileAccess.Read);
                disks.Add(disk);
                if (view != null && view.Length != disk.Capacity)
                    throw new InvalidDataException("Parent and child virtual sizes differ");
                view = disk.OpenContent(view, Ownership.None);
                streams.Add(view);
            }
            Console.Error.WriteLine("Independent reference: " + typeof(DiskImageFile).Assembly.FullName);
            Console.Error.WriteLine("Virtual bytes: " + view.Length);
            var buffer = new byte[1024 * 1024];
            using (var output = new BinaryWriter(Console.OpenStandardOutput()))
            {
                output.Write(Encoding.ASCII.GetBytes("VHDXRAW1"));
                output.Write((ulong)view.Length);
                long offset = 0;
                while (offset < view.Length)
                {
                    int count = (int)Math.Min(buffer.Length, view.Length - offset);
                    int received = view.Read(buffer, 0, count);
                    if (received != count)
                        throw new EndOfStreamException("Short reference read at " + offset);
                    output.Write(buffer, 0, count);
                    offset += count;
                }
                if (view.Read(buffer, 0, 512) != 0)
                    throw new InvalidDataException("Reference exceeds its virtual size");
            }
            return 0;
        }
        catch (Exception error)
        {
            Console.Error.WriteLine(error);
            return 1;
        }
        finally
        {
            for (int i = streams.Count - 1; i >= 0; --i)
                streams[i].Dispose();
            for (int i = disks.Count - 1; i >= 0; --i)
                disks[i].Dispose();
        }
    }
}
