open System
open Amazon.S3
open AssetQL
open AssetQL.TestTypes
open AssetQL.Cryptography
open AssetQL.Models
[<EntryPoint>]
let main argv =

    // printfn "%s" (Operations.decompressString (Operations.compressString "hello world"))

    let bucketName = "test-bucket"
    let s3config = AmazonS3Config(ServiceURL = "http://localhost:9000", ForcePathStyle = true)
    let client = new AmazonS3Client("ACCESS_KEY", "SECRET_KEY", s3config)
    let table:AssetTable<A> = {Client = client; Name = bucketName; Encryption = AES256 (Aes.GenerateKey()); Compression = GZip; Version = "1.1"}
    (Operations.put table {primary = "testHash"; query = "testQuery"; content = "content"} (Some (fun x -> true)) false).Wait()
    // printfn "%O" ((Operations.query {Client = client; Name = bucketName} "testQuery").Result.[0] )
    // printfn "%O" ((Operations.get table (TableKey.Combined("testHash","testQuery"))).Result )
    // printfn "%O" ((Operations.update<A> table (TableKey.Combined("testHash","testQuery")) (fun x -> {x with primary = "someOther"}) None None).Result )

    0
