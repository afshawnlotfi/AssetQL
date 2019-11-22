namespace AssetData

open Newtonsoft.Json
open Amazon.S3.Model
open System.Threading.Tasks


module Models =
    open Amazon.S3
    open System.Reflection

    type AssetTable<'t> =
        { BucketName: string
          Client: IAmazonS3 }

    type QueryKeyAttribute() =
        inherit System.Attribute()


    type PrimaryKeyAttribute() =
        inherit System.Attribute()

    type PropertyModel =
        { PropertyName: string
          PropertyValue: string }


    type TableKey =
        { QueryKey: string option
          PrimaryKey: string }



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



    let getPropertyValue object (property: PropertyInfo) =
        let value = property.GetValue object
        if value.GetType().FullName = "System.String" then value.ToString()
        else failwith (sprintf "Property must be of type 'System.String'")



module TestTypes =
    open Models

    type A =
        { [<PrimaryKey>]
          primary: string
          [<QueryKey>]
          query: string }

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
    open System.Text
    open System.IO
    open Models
    open Amazon.S3.Transfer


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


    let internal pathFromTableKey tableKey =
        if tableKey.QueryKey.IsSome then sprintf "%s/%s" tableKey.QueryKey.Value tableKey.PrimaryKey
        else tableKey.PrimaryKey


    let query ({ Client = client; BucketName = bucketName }: AssetTable<'t>) queryKey =
        task {

            let! listed = client.ListObjectsV2Async
                              (ListObjectsV2Request(BucketName = bucketName, Prefix = sprintf "/%s" queryKey))

            return listed.S3Objects.ToArray()
                   |> Array.filter (fun object -> object.StorageClass = S3StorageClass.Standard)
                   |> Array.map (fun object -> object.Key.Replace(sprintf "/%s/" queryKey, ""))
        }

    let get<'t> ({ Client = client; BucketName = bucketName }: AssetTable<'t>) (tableKey: TableKey) =
        task {
            let key = pathFromTableKey tableKey
            let! getResponse = client.GetObjectAsync(bucketName, key)
            use reader = new StreamReader(getResponse.ResponseStream)
            let body = reader.ReadToEnd()
            return JsonConvert.DeserializeObject<'t> body
        }


    let batchGet<'t> (table: AssetTable<'t>) (tableKeys: TableKey []) =
        let getTasks = tableKeys |> Array.map (fun tableKey -> get table tableKey)
        getTasks |> Task.WhenAll


    let put (table: AssetTable<'t>) (object: 't) (isQueryable: ('t -> bool) option) (assertItemDoesNotExist: bool) =
        let { Client = client; BucketName = bucketName } = table
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

            if assertItemDoesNotExist then
                try
                    let! _ = get table tableKey
                    failwith "Item exists when assertItemDoesNotExist paramter given"
                with _ -> ()


            let byteArray = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject object)
            let stream = new MemoryStream(byteArray)
            use transferUtility = new TransferUtility(client)
            let key = pathFromTableKey tableKey
            let request =
                TransferUtilityUploadRequest
                    (InputStream = stream, BucketName = bucketName,
                     StorageClass =
                         (if (isQueryable.IsSome && isQueryable.Value object) || isQueryable.IsNone then
                             S3StorageClass.Standard
                          else S3StorageClass.ReducedRedundancy), Key = key)
            do! transferUtility.UploadAsync(request)
            return tableKey
        }


    let delete (table: AssetTable<'t>) (tableKey: TableKey) (precondition: ('t -> bool) option) =
        task {
            let { Client = client; BucketName = bucketName } = table
            let path = pathFromTableKey tableKey
            let! object = get table tableKey
            if precondition.IsSome then
                if precondition.Value object then
                    let! _ = client.DeleteObjectAsync(bucketName, path)
                    return object
                else return failwith "precondition failed"
            else
                let! _ = client.DeleteObjectAsync(bucketName, path)
                return object
        }



    let update<'t> (table: AssetTable<'t>) (tableKey: TableKey) (expr: 't -> 't) (precondition: ('t -> bool) option)
        (isQueryable: ('t -> bool) option) =
        task {
            let { Client = client; BucketName = bucketName } = table
            let! oldObject = get<'t> table tableKey
            if (precondition.IsSome && precondition.Value oldObject) then

                let oldObjectKey = keyFromType oldObject
                let newObject = (expr oldObject)
                let newObjectKey = keyFromType newObject

                if oldObjectKey <> newObjectKey then 
                    let! _ = delete table tableKey None; 
                    ();

                let! _ = put table newObject isQueryable false
                return newObject
            else
                return failwith "precondition failed"
        }
