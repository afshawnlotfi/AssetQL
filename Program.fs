open System
open Amazon.S3
open AssetData
open AssetData.TestTypes
[<EntryPoint>]
let main argv =
    let bucketName = "test-bucket"
    let s3config = AmazonS3Config(ServiceURL = "http://localhost:9000", ForcePathStyle = true)
    let client = new AmazonS3Client("ACCESS_KEY", "SECRET_KEY", s3config)
    
    // (client.PutBucketAsync bucketName).Wait()

    (Operations.put {Client = client; BucketName = bucketName} {primary = "testHash"; query = "testQuery"} (Some (fun x -> true)) false).Wait()
    printfn "%O" ((Operations.query {Client = client; BucketName = bucketName} "testQuery").Result.[0] )
    // printfn "%O" ((Operations.get client bucketName (TableKey.Combined("testHash","testQuery"))).Result )
    // printfn "%O" ((Operations.update<A> client bucketName (TableKey.Combined("testHash","testQuery")) (fun x -> {x with primary = "someOther"})).Result )

    0
