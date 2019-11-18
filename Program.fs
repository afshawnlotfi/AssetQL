open System
open Amazon.S3
open AssetData
open AssetData.TestModels
[<EntryPoint>]
let main argv =
    let bucketName = "test-bucket"
    let s3config = AmazonS3Config(ServiceURL = "http://localhost:9000", ForcePathStyle = true)
    let client = new AmazonS3Client("ACCESS_KEY", "SECRET_KEY", s3config)
    
    // (client.PutBucketAsync bucketName).Wait()

    // (Operations.uploadObject client bucketName {hash = "testHash"; query = "testQuery"}).Wait()
    // printfn "%O" ((Operations.queryObjects client bucketName "testQuery").Result.[0] )
    printfn "%O" ((Operations.getObject client bucketName (TableKey.Combined("testHash","testQuery"))).Result )

    0
