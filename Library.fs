﻿namespace AssetQL

open Newtonsoft.Json
open Amazon.S3.Model
open System.Threading.Tasks
open System.IO
open AssetQL
open System.Text
open System.Text.RegularExpressions

module Models =
    open Amazon.S3
    open System.Reflection


    type CompressionType =
        | GZip
        | NoCompression

    type EncryptionType =
        | AES256 of Cryptography.CryptoKey
        | NoEncryption



    type AssetTable<'t> =
        { Name: string
          Version: string
          Client: IAmazonS3
          Encryption: EncryptionType
          Compression: CompressionType }

    type QueryKeyAttribute() =
        inherit System.Attribute()

    type TTLAttribute() =
        inherit System.Attribute()


    type PrimaryKeyAttribute() =
        inherit System.Attribute()

    type PropertyModel =
        { PropertyName: string
          PropertyValue: string }


    type TableKey =
        { QueryKey: string option
          PrimaryKey: string }



    let encode (src: string) (version : string) =
        let plainTextBytes = System.Text.Encoding.UTF8.GetBytes(src)
        sprintf "VERSION_%s_%s" version (System.Convert.ToBase64String(plainTextBytes).Replace("+", "-").Replace("/", "_").Replace("=", "!"))

    let decode (src: string) =
        let m = Regex.Match(src, "VERSION_(..*)_(..*)")
        let base64 = m.Groups.[2].Value
        let base64EncodedBytes =
            System.Convert.FromBase64String(base64.Replace("-", "+").Replace("_", "/").Replace("!", "="))
        System.Text.Encoding.UTF8.GetString(base64EncodedBytes)

    let getProperty (objectType: System.Type) keyName =
        let mutable keyProperty: PropertyInfo option = None
        objectType.GetProperties()
        |> Array.iter
            (fun property ->
            (property.CustomAttributes
             |> Seq.iter
                 (fun attribute ->
                 let attributeName = attribute.AttributeType.Name
                 if attributeName = keyName then
                     if keyProperty.IsNone then keyProperty <- property |> Some
                     else
                         failwith
                             (sprintf "%s was already assigned for %s. Only one property can be used as a primary key"
                                  keyName keyProperty.Value.Name))))

        match keyProperty with
        | (Some property) -> Some property
        | _ -> None


    let getPrimaryProperty objectType =
        let property = getProperty objectType (typedefof<PrimaryKeyAttribute>).Name
        if property.IsSome then property.Value
        else failwith "PrimaryKey was not assigned"

    let getQueryProperty objectType = getProperty objectType (typedefof<QueryKeyAttribute>).Name

    let getTTLProperty objectType = getProperty objectType (typedefof<TTLAttribute>).Name

    let getPropertyValue object (property: PropertyInfo) =
        let value = property.GetValue object
        if value.GetType().FullName = "System.String" then value.ToString()
        else failwith (sprintf "Property must be of type 'System.String'")

    let getTTLValue object (property: PropertyInfo) =
        let value = property.GetValue object
        if value.GetType().FullName = "System.Int64" then value :?> int64
        else failwith (sprintf "TTL must be of type 'System.Int64'")

module TestTypes =
    open Models

    type A =
        { [<PrimaryKey>]
          primary: string
          [<QueryKey>]
          query: string
          content: string }

module TableKey =
    open Models

    let PrimaryKey primary =
        { PrimaryKey = primary
          QueryKey = None }

    let Combined((primary, query): string * string) =
        { PrimaryKey = primary
          QueryKey = Some query }

module Operations =
    open Amazon.S3
    open FSharp.Control.Tasks.V2
    open Models
    open Amazon.S3.Transfer

    let mutable ttlCheckEnabled = true


    let keyFromType object =
        let primaryKey = getPrimaryProperty (object.GetType()) |> getPropertyValue object

        let queryKey =
            let queryProperty = getQueryProperty (object.GetType())
            if queryProperty.IsSome then
                queryProperty.Value
                |> getPropertyValue object
                |> Some
            else
                None
        { PrimaryKey = primaryKey
          QueryKey = queryKey }


    let internal pathFromTableKey tableKey version =
        if tableKey.QueryKey.IsSome then sprintf "%s/%s" (encode tableKey.QueryKey.Value version) (encode tableKey.PrimaryKey version)
        else (encode tableKey.PrimaryKey version)



    let query ({ Client = client; Name = tableName; Version = version }: AssetTable<'t>) queryKey =
        task {
            let encodedQueryKey = encode queryKey version
            let! listed = client.ListObjectsV2Async
                              (ListObjectsV2Request(BucketName = tableName, Prefix = sprintf "%s" encodedQueryKey))

            return listed.S3Objects.ToArray()
                   |> Array.filter (fun object -> object.StorageClass = S3StorageClass.Standard)
                   |> Array.map (fun object -> decode (object.Key.Replace(sprintf "%s/" encodedQueryKey, "")))
        }





    let isExpired object =
        let ttlProperty = getTTLProperty (object.GetType())

        if ttlProperty.IsSome then
            let ttl = ttlProperty.Value |> getTTLValue object
            ttlCheckEnabled && ttl < System.DateTimeOffset.UtcNow.ToUnixTimeSeconds()
        else
            failwith "TTL property does not exist"


    let internal getWithoutException ({ Client = client; Name = tableName; Compression = compression;
                                        Encryption = encryption; Version = version }: AssetTable<'t>) (tableKey: TableKey) =
        task {
            try
                let path = pathFromTableKey tableKey version
                let! getResponse = client.GetObjectAsync(tableName, path)
                use stream = new MemoryStream()
                getResponse.ResponseStream.CopyTo(stream)



                let decrypted =
                    match encryption with
                    | AES256 encodedCryptoKey -> Cryptography.Aes.Decrypt(stream.ToArray(), encodedCryptoKey)
                    | NoEncryption -> stream.ToArray()

                let uncompressed =
                    match compression with
                    | GZip -> Compression.GZip.Unzip(decrypted)
                    | NoCompression -> decrypted



                let object = JsonConvert.DeserializeObject<'t> (Encoding.UTF8.GetString uncompressed)

                try
                    if isExpired object then
                        let! _ = client.DeleteObjectAsync(tableName, path)
                        return None
                    else return object |> Some
                with _ -> return object |> Some

            with _ -> return None
        }


    let get<'t> (table: AssetTable<'t>) (tableKey: TableKey) =
        task {
            let! getResponse = getWithoutException table tableKey
            if getResponse.IsSome then return getResponse.Value
            else return failwith (sprintf "failed to get spicifc table key %s" (tableKey.ToString()))
        }


    let batchGet<'t> (table: AssetTable<'t>) (tableKeys: TableKey []) =
        let getTasks = tableKeys |> Array.map (fun tableKey -> getWithoutException table tableKey)
        task {
            let! responses = getTasks |> Task.WhenAll
            return seq {
                       for response in responses do
                           if response.IsSome then yield response.Value
                   }
                   |> Seq.toArray
        }


    let upload ({ Client = client; Name = tableName; Compression = compression; Encryption = encryption; Version = version }: AssetTable<'t>)
        (tableKey: TableKey) (byteArray: byte []) (storageClass: S3StorageClass option) =
        task {
            use transferUtility = new TransferUtility(client)
            let key = pathFromTableKey tableKey version


            let compressed =
                match compression with
                | GZip -> Compression.GZip.Zip(byteArray)
                | NoCompression -> byteArray

            let encrypted =
                match encryption with
                | AES256 encodedCryptoKey -> Cryptography.Aes.Encrypt(compressed, encodedCryptoKey)
                | NoEncryption -> compressed


            let request =
                TransferUtilityUploadRequest
                    (InputStream = new MemoryStream(encrypted), BucketName = tableName, Key = key,
                     StorageClass =
                         if storageClass.IsSome then storageClass.Value
                         else S3StorageClass.Standard)

            do! transferUtility.UploadAsync(request)
            return tableKey
        }



    let put (table: AssetTable<'t>) (object: 't) (isQueryable: ('t -> bool) option) (assertItemDoesNotExist: bool) =
        task {
            let primaryKey = (getPrimaryProperty (object.GetType())) |> getPropertyValue object
            let queryProperty = (getQueryProperty (object.GetType()))

            let queryKey =
                if queryProperty.IsSome then
                    queryProperty.Value
                    |> getPropertyValue object
                    |> Some
                else
                    None

            let tableKey =
                { PrimaryKey = primaryKey
                  QueryKey = queryKey }


            let inferItemDoesNotExist =
                try
                    isExpired object // if expired item is there but needs to be overwritten so there is no point in checking if item exists for assertion in next block; Expired = itemDoesNotExist
                with _ -> false

            if assertItemDoesNotExist && (not inferItemDoesNotExist) then

                let! exists = task {
                                  try
                                      let! _ = get table tableKey
                                      return true
                                  with _ -> return false
                              }

                if exists then failwith "Item exists when assertItemDoesNotExist parameter given"


            let byteArray = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject object)
            let stream = new MemoryStream(byteArray)

            let storageClass =
                (if (isQueryable.IsSome && isQueryable.Value object) || isQueryable.IsNone then S3StorageClass.Standard
                 else S3StorageClass.ReducedRedundancy)

            return! upload table tableKey byteArray (Some storageClass)
        }


    let delete (table: AssetTable<'t>) (tableKey: TableKey) (precondition: ('t -> bool) option) =
        task {
            let { Client = client; Name = tableName; Version = version } = table
            let path = pathFromTableKey tableKey version
            let! object = get table tableKey
            if precondition.IsSome then
                if precondition.Value object then
                    let! _ = client.DeleteObjectAsync(tableName, path)
                    return object
                else return failwith "precondition failed"
            else
                let! _ = client.DeleteObjectAsync(tableName, path)
                return object
        }



    let update<'t> (table: AssetTable<'t>) (tableKey: TableKey) (expr: 't -> 't) (precondition: ('t -> bool) option)
        (isQueryable: ('t -> bool) option) =
        task {
            let! oldObject = get<'t> table tableKey
            if ((precondition.IsSome && precondition.Value oldObject) || precondition.IsNone) then

                let oldObjectKey = keyFromType oldObject
                let newObject = (expr oldObject)
                let newObjectKey = keyFromType newObject

                if oldObjectKey <> newObjectKey then
                    let! _ = delete table tableKey None
                    ()

                let! _ = put table newObject isQueryable false
                return newObject
            else
                return failwith "precondition failed"
        }



    let getAccessUrl ({ Client = client; Name = tableName; Version = version }: AssetTable<'t>) (tableKey: TableKey) duration =
        let request =
            GetPreSignedUrlRequest
                (BucketName = tableName, Key = (pathFromTableKey tableKey version), Verb = HttpVerb.GET, Protocol = Protocol.HTTP,
                 Expires = duration)
        client.GetPreSignedURL(request)


    let postAccessUrl ({ Client = client; Name = tableName; Version = version }: AssetTable<'t>) (tableKey: TableKey) duration contentType =
        let request =
            GetPreSignedUrlRequest
                (BucketName = tableName, Key = pathFromTableKey tableKey version, Expires = duration, Verb = HttpVerb.PUT,
                 Protocol = Protocol.HTTP, ContentType = contentType)
        client.GetPreSignedURL(request)
