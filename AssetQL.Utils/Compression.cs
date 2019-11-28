using System.Text;
using System.IO;
using System.IO.Compression;

namespace AssetQL.Compression
{
    public class GZip
    {
        public static byte[] Zip(byte[] bytes)
        {
            // var bytes = Encoding.UTF8.GetBytes(str);

            using (var msi = new MemoryStream(bytes))
            {
                using (var mso = new MemoryStream())
                {
                    using (var gs = new GZipStream(mso, CompressionMode.Compress))
                    {
                        msi.CopyTo(gs);
                    }
                    return mso.ToArray();
                }
            }
        }

        public static byte[] Unzip(byte[] bytes)
        {

            using (var msi = new MemoryStream(bytes))
            {

                using (var mso = new MemoryStream())
                {
                    using (var gs = new GZipStream(msi, CompressionMode.Decompress))
                    {
                        gs.CopyTo(mso);
                    }

                    return mso.ToArray();
                }
            }
        }

    }
}