Avro Schema GeneratorA lightweight C# library for generating Apache Avro schemas (in JSON format) and converting POCO instances to Avro's GenericRecord using reflection.This library is useful for integrating C# models with Apache Kafka, Schema Registry, or other systems that rely on the Avro data format.FeaturesSchema Generation: Automatically creates a valid Avro Record Schema JSON string from any C# class.Type Support: Handles primitives, enums, DateTime (as timestamp-millis), nullable types, and nested complex objects.POCO Conversion: Easily convert your instantiated C# objects into Avro.Generic.GenericRecord objects, correctly handling type conversions like DateTime to long timestamps.InstallationThe library is available on NuGet.Install-Package AvroSchema.Generator
# Or using dotnet CLI
dotnet add package AvroSchema.Generator
Usage1. Generating a Schemausing AvroSchemaGeneration;
using System;

// Define your model
public class Customer
{
    public Guid Id { get; set; } // Note: Not directly supported yet, will need a custom extension or bytes.
    public string Name { get; set; }
    public int OrderCount { get; set; }
    public DateTime LastPurchase { get; set; }
    public bool? IsPremium { get; set; }
}

// Generate the schema
Type customerType = typeof(Customer);
string avroSchemaJson = AvroSchemaGenerator.GenerateSchema(customerType);

Console.WriteLine(avroSchemaJson);
/* Output (Simplified):
{ 
    "type": "record", 
    "name": "Customer", 
    "namespace": "YourNamespace", 
    "fields": [ 
        { "name": "Id", "type": ["null", "string"], "default": null }, // Assuming Guid is mapped to string for now
        { "name": "Name", "type": ["null", "string"], "default": null }, 
        { "name": "OrderCount", "type": "int", "default": 0 },
        { "name": "LastPurchase", "type": { "type": "long", "logicalType": "timestamp-millis" }, "default": 0 },
        { "name": "IsPremium", "type": ["null", "boolean"], "default": null }
    ] 
}
*/
2. Converting POCO to GenericRecordTo use the conversion utility, you need to parse the generated schema using the Apache.Avro library first.using Avro.Schema;
using Avro.Generic;
using AvroSchemaGeneration;

// 1. Define POCO instance
var customerPoco = new Customer 
{ 
    Name = "Alice", 
    OrderCount = 5, 
    LastPurchase = DateTime.UtcNow,
    IsPremium = true
};

// 2. Get and Parse Schema
var schemaJson = AvroSchemaGenerator.GenerateSchema(customerPoco.GetType());
var recordSchema = Schema.Parse(schemaJson) as RecordSchema;

// 3. Convert
GenericRecord avroRecord = AvroSchemaGenerator.ConvertToGenericRecord(customerPoco, recordSchema);

// avroRecord is now ready for serialization with an Avro writer
