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
        | (Some property) -> property
        | _ -> failwith (sprintf "No %s attribute was provided in object" keyName)


    let getPrimaryProperty objectType = getProperty objectType (typedefof<PrimaryKeyAttribute>).Name

    let getQueryProperty objectType = getProperty objectType (typedefof<QueryKeyAttribute>).Name



    let getPropertyValue object (property: PropertyInfo) =
        let value = property.GetValue object
        if value.GetType().FullName = "System.String" then value.ToString()
        else failwith (sprintf "Property must be of type 'System.String'")


module TableKey =
    open Models

    let PrimaryKey primary =
        { PrimaryKey = primary
          QueryKey = None }

    let Combined((primary, query): string * string) =
        { PrimaryKey = primary
          QueryKey = Some query }

module TestModels =
    open Models

    type A =
        { [<PrimaryKey>]
          hash: string
          [<QueryKey>]
          query: string }


module Operations =
    open Amazon.S3
    open FSharp.Control.Tasks.V2
    open System.Text
    open System.IO
    open Models
    open Amazon.S3.Transfer



    let internal queryObjects (client: IAmazonS3) bucketName queryName =
        task {
            let! listed = client.ListObjectsAsync(bucketName, sprintf "/%s" queryName)
            return listed.S3Objects.ToArray() |> Array.map (fun object -> object.Key) }



    let initalPath (object: obj) =
        try
            let queryPropertyValue = getQueryProperty (object.GetType()) |> getPropertyValue object
            sprintf "%s/" queryPropertyValue
        with _ -> ""

    let internal getObject<'t> (client: IAmazonS3) bucketName (tableKey: TableKey) =
        task {
            let initalPath =
                if tableKey.QueryKey.IsSome then sprintf "%s/" tableKey.QueryKey.Value
                else ""
            let! getResponse = client.GetObjectAsync(bucketName, initalPath + tableKey.PrimaryKey)
            use reader = new StreamReader(getResponse.ResponseStream)
            let body = reader.ReadToEnd()
            return JsonConvert.DeserializeObject<'t> body
        }


    let internal uploadObject (client: IAmazonS3) bucketName (object: obj) =
        task {
            let primaryPropertyValue = (getPrimaryProperty (object.GetType())) |> getPropertyValue object
            let byteArray = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject object)
            let stream = new MemoryStream(byteArray)
            use transferUtility = new TransferUtility(client)
            do! transferUtility.UploadAsync(stream, bucketName, (initalPath object) + primaryPropertyValue)
        }




    let Get = ()
    let Write = ()
    let Query = ()
