using System;
using System.IO;
using System.Security.Cryptography;
using System.Linq;
using System.Text;

namespace AssetQL.Cryptography
{


    public class CryptoKey
    {
        public byte[] IV { get; set; }
        public byte[] Key { get; set; }

    }

    public class Aes
    {

        public static CryptoKey ImportKey(string encodedUnifiedKey)
        {
            byte[] unifiedKey = Convert.FromBase64String(encodedUnifiedKey);
            byte[] iv = unifiedKey.Take(16).ToArray();
            byte[] key = unifiedKey.Skip(16).Take(32).ToArray();
            return new CryptoKey { IV = iv, Key = key };
        }


        public static string ExportKey(CryptoKey cryptoKey)
        {
            byte[] unifiedKey = cryptoKey.IV.Concat(cryptoKey.Key).ToArray();
            return Convert.ToBase64String(unifiedKey);
        }



        public static CryptoKey GenerateKey()
        {

            // Create a new instance of the AesManaged
            // class.  This generates a new key and initialization 
            // vector (IV).
            using (AesManaged aes = new AesManaged())
            {
                return new CryptoKey { IV = aes.IV, Key = aes.Key };
            }
        }




        public static void Main()
        {
            string original = "Here is some data to encrypt!";

            // convert string to stream
            byte[] byteArray = System.Text.Encoding.UTF8.GetBytes(original);
            //byte[] byteArray = Encoding.ASCII.GetBytes(contents);
            // MemoryStream originalStream = new MemoryStream(byteArray);


            // Create a new instance of the AesManaged
            // class.  This generates a new key and initialization 
            // vector (IV).
            using (AesManaged myAes = new AesManaged())
            {
                CryptoKey cryptoKey = GenerateKey();

                // Encrypt the string to an array of bytes.
                byte[] encrypted = Encrypt(byteArray, cryptoKey);

                // Decrypt the bytes to a string.
                byte[] roundtrip = Decrypt(encrypted, ImportKey(ExportKey(cryptoKey)));

                //Display the original data and the decrypted data.
                Console.WriteLine("Original:   {0}", original);
                Console.WriteLine("Round Trip: {0}", Encoding.UTF8.GetString(roundtrip));
            }
        }



        public static byte[] Encrypt(byte[] bytes, CryptoKey cryptoKey)
        {
            byte[] Key = cryptoKey.Key;
            byte[] IV = cryptoKey.IV;
            // Check arguments.
            if (bytes == null || bytes.Length <= 0)
                throw new ArgumentNullException("bytes");
            if (Key == null || Key.Length <= 0)
                throw new ArgumentNullException("Key");
            if (IV == null || IV.Length <= 0)
                throw new ArgumentNullException("IV");

            // Create an AesManaged object
            // with the specified key and IV.
            using (AesManaged aesAlg = new AesManaged())
            {
                aesAlg.Key = Key;
                aesAlg.IV = IV;

                // Create an encryptor to perform the stream transform.
                ICryptoTransform encryptor = aesAlg.CreateEncryptor(aesAlg.Key, aesAlg.IV);

                // Create the streams used for encryption.
                using (MemoryStream msEncrypt = new MemoryStream())
                {
                    using (CryptoStream csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write))
                    {
                        csEncrypt.Write(bytes);
                    }
                    return msEncrypt.ToArray();

                }
            }

        }

        public static byte[] Decrypt(byte[] cipherBytes, CryptoKey cryptoKey)
        {
            byte[] Key = cryptoKey.Key;
            byte[] IV = cryptoKey.IV;

            // Check arguments.
            if (cipherBytes == null || cipherBytes.Length <= 0)
                throw new ArgumentNullException("cipherBytes");
            if (Key == null || Key.Length <= 0)
                throw new ArgumentNullException("Key");
            if (IV == null || IV.Length <= 0)
                throw new ArgumentNullException("IV");

            // Declare the string used to hold
            // the decrypted text.
            byte[] decrypted = null;

            // Create an AesManaged object
            // with the specified key and IV.
            using (AesManaged aesAlg = new AesManaged())
            {
                aesAlg.Key = Key;
                aesAlg.IV = IV;

                // Create a decryptor to perform the stream transform.
                ICryptoTransform decryptor = aesAlg.CreateDecryptor(aesAlg.Key, aesAlg.IV);

                // Create the streams used for decryption.
                using (MemoryStream msDecrypt = new MemoryStream(cipherBytes))
                {
                    using (CryptoStream csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read))
                    {
                        using (MemoryStream srDecrypt = new MemoryStream())
                        {
                            csDecrypt.CopyTo(srDecrypt);
                            // Read the decrypted bytes from the decrypting stream
                            // and place them in a string.
                            decrypted = srDecrypt.ToArray();

                        }
                        return decrypted;
                    }
                }

            }


        }
    }
}