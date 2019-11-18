namespace AssetData

open Newtonsoft.Json


module Models =

    open System.Reflection

    type TableModel =
        { Name: string }

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
    type A = {
        [<PrimaryKey>]
        primary : string
        [<QueryKey>]
        query : string
    }

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


    let query (client: IAmazonS3) bucketName queryName =
        task {
            let! listed = client.ListObjectsAsync(bucketName, sprintf "/%s" queryName)
            return listed.S3Objects.ToArray() |> Array.map (fun object -> object.Key) }

    let get<'t> (client: IAmazonS3) bucketName (tableKey: TableKey) =
        task {
            let! getResponse = client.GetObjectAsync(bucketName, pathFromTableKey tableKey)
            use reader = new StreamReader(getResponse.ResponseStream)
            let body = reader.ReadToEnd()
            return JsonConvert.DeserializeObject<'t> body
        }


    let put (client: IAmazonS3) bucketName (object: obj) =
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

            let byteArray = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject object)
            let stream = new MemoryStream(byteArray)
            use transferUtility = new TransferUtility(client)
            do! transferUtility.UploadAsync
                    (stream, bucketName,
                     pathFromTableKey
                         { PrimaryKey = primaryKey
                           QueryKey = queryKey })
        }


    let update<'t> (client: IAmazonS3) bucketName (tableKey: TableKey) (expr: 't -> 't) =
        task {
            let! oldObject = get<'t> client bucketName tableKey
            let oldObjectKey = keyFromType oldObject
            let newObject = (expr oldObject)
            let newObjectKey = keyFromType newObject

            if oldObjectKey <> newObjectKey then
                let path = pathFromTableKey tableKey
                let! _ = client.DeleteObjectAsync(bucketName, path)
                ()

            do! put client bucketName newObject
        }
