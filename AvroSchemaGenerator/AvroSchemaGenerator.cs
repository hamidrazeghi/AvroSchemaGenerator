using System.Reflection;
using System.Text;
using Avro.Specific;
using Avro.Generic;
using Avro;


namespace AvroSchemaGeneration
{
    /// <summary>
    /// Generates Apache Avro Schemas (JSON strings) from C# POCO types using reflection.
    /// It also provides functionality to convert POCO instances into Avro GenericRecords.
    /// </summary>
    public static class AvroSchemaGenerator
    {
        /// <summary>
        /// Generates the Avro Schema JSON string for a given C# Type.
        /// </summary>
        /// <param name="type">The C# type to generate the schema for.</param>
        /// <returns>A JSON string representing the Avro schema.</returns>
        public static string GenerateSchema(Type type)
        {
            return GenerateSchemaFromType(type);
        }

        private static string GenerateSchemaFromType(Type type)
        {
            // Handle primitive types
            if (type == typeof(string)) return "\"string\"";
            if (type == typeof(int) || type == typeof(Int32)) return "\"int\"";
            if (type == typeof(long) || type == typeof(Int64)) return "\"long\"";
            if (type == typeof(bool)) return "\"boolean\"";
            if (type == typeof(float)) return "\"float\"";
            if (type == typeof(double)) return "\"double\"";
            if (type == typeof(byte[])) return "\"bytes\"";

            // Handle Decimal (using string or fixed/bytes with logical type, choosing string for simplicity)
            if (type == typeof(decimal)) return "\"string\"";

            // Handle DateTime with logical type (long timestamp in milliseconds)
            if (type == typeof(DateTime))
            {
                return "{ \"type\": \"long\", \"logicalType\": \"timestamp-millis\" }";
            }

            // Handle nullable types (e.g., int?)
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                var underlyingType = Nullable.GetUnderlyingType(type);
                var nestedSchema = GenerateSchemaFromType(underlyingType);
                // Nullable types are represented as a union of ["null", actual_type]
                return $"[\"null\", {nestedSchema}]";
            }

            // Handle Enums
            if (type.IsEnum)
            {
                var enumName = type.Name;
                // Using FullName for better unique identification, fallback to Namespace if null
                var enumNamespace = type.Namespace ?? "global";
                var symbols = string.Join(", ", Enum.GetNames(type).Select(s => $"\"{s}\""));
                return $"{{ \"type\": \"enum\", \"name\": \"{enumName}\", \"namespace\": \"{enumNamespace}\", \"symbols\": [{symbols}] }}";
            }


            // Handle Arrays/Lists (as Avro arrays)
            if (type.IsArray)
            {
                var elementType = type.GetElementType();
                var elementSchema = GenerateSchemaFromType(elementType);
                return $"{{ \"type\": \"array\", \"items\": {elementSchema} }}";
            }
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
            {
                var elementType = type.GetGenericArguments()[0];
                var elementSchema = GenerateSchemaFromType(elementType);
                return $"{{ \"type\": \"array\", \"items\": {elementSchema} }}";
            }

            // Handle complex types (classes/records)
            if (type.IsClass && type != typeof(string))
            {
                var sb = new StringBuilder();
                // Using FullName for better record identification
                var fullTypeName = type.FullName ?? type.Name;
                var recordName = type.Name;
                var recordNamespace = type.Namespace ?? "global";

                sb.Append("{ \"type\": \"record\", \"name\": \"").Append(recordName).Append("\", ");
                sb.Append("\"namespace\": \"").Append(recordNamespace).Append("\", ");
                sb.Append("\"fields\": [ ");

                var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                    .Where(p => p.GetGetMethod() != null); // Ensure it's readable

                var fields = properties.Select(prop =>
                {
                    var propertyType = prop.PropertyType;
                    var isReferenceType = propertyType.IsClass && propertyType != typeof(string) && !propertyType.IsArray;
                    var isNullableValueType = propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>);
                    var isString = propertyType == typeof(string);

                    string fieldSchema;
                    string defaultValue;

                    // Nullable logic: Avro reference types (like string, other records) and C# Nullable<T> are unioned with "null".
                    if (isNullableValueType || isReferenceType || isString)
                    {
                        Type typeToGenerate = isNullableValueType ? Nullable.GetUnderlyingType(propertyType) : propertyType;
                        var nestedSchema = GenerateSchemaFromType(typeToGenerate);

                        fieldSchema = $"[\"null\", {nestedSchema}]";
                        defaultValue = $"\"default\": null";
                    }
                    else
                    {
                        // Non-Nullable Value Types (int, bool, DateTime, etc.):
                        fieldSchema = GenerateSchemaFromType(propertyType);
                        
                        if (propertyType == typeof(int) || propertyType == typeof(long))
                        {
                            defaultValue = $"\"default\": 0";
                        }
                        else if (propertyType == typeof(bool))
                        {
                            defaultValue = $"\"default\": false";
                        }
                        else if (propertyType == typeof(float) || propertyType == typeof(double))
                        {
                            defaultValue = $"\"default\": 0.0";
                        }
                        else if (propertyType == typeof(byte[]) || propertyType == typeof(decimal))
                        {
                            // Avro bytes/string default must be a string literal for JSON
                            defaultValue = $"\"default\": \"\"";
                        }
                        else if (propertyType == typeof(DateTime))
                        {
                            // DateTime logical type is long, default should be 0 (epoch start)
                            defaultValue = $"\"default\": 0";
                        }
                        else if (propertyType.IsEnum)
                        {
                            // Enums must default to one of the symbols, use the first symbol name.
                            defaultValue = $"\"default\": \"{Enum.GetNames(propertyType).First()}\"";
                        }
                        else
                        {
                            // Fallback for other non-nullable value types
                            defaultValue = $"\"default\": 0";
                        }
                    }

                    return $"{{ \"name\": \"{prop.Name}\", \"type\": {fieldSchema}, {defaultValue} }}";
                });

                sb.Append(string.Join(", ", fields));
                sb.Append(" ] }");

                return sb.ToString();
            }
            throw new NotSupportedException($"Type {type.Name} is not yet supported for Avro schema generation.");
        }


        /// <summary>
        /// Converts a POCO object into an Apache Avro GenericRecord, using a provided RecordSchema.
        /// </summary>
        /// <param name="poco">The POCO instance to convert.</param>
        /// <param name="schema">The Avro RecordSchema generated for the POCO type.</param>
        /// <returns>A populated Avro GenericRecord.</returns>
        public static GenericRecord ConvertToGenericRecord(object poco, RecordSchema schema)
        {
            var avroRecord = new GenericRecord(schema);
            var pocoType = poco.GetType();

            foreach (var field in schema.Fields)
            {
                var pocoProperty = pocoType.GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance);
                if (pocoProperty != null)
                {
                    var value = pocoProperty.GetValue(poco);

                    // Handle null values
                    if (value == null)
                    {
                        avroRecord.Add(field.Name, null);
                        continue;
                    }

                    // Special handling for logical types (DateTime to long timestamp-millis)
                    if (field.Schema is LogicalSchema logicalSchema && logicalSchema.Name == "timestamp-millis")
                    {
                        if (value is DateTime dateTimeValue)
                        {
                            value = new DateTimeOffset(dateTimeValue.ToUniversalTime()).ToUnixTimeMilliseconds();
                        }
                    }
                    else if (value is decimal decimalValue)
                    {
                        value = decimalValue.ToString();
                    }
                    else if (value.GetType().IsEnum)
                    {
                        // Enums are stored as strings of their name in Avro
                        value = value.ToString();
                    }
                    else if (value is ICollection<object> collection)
                    {
                        // Future implementation for recursive conversion of nested POCOs in arrays
                        // For now, assume simple collections or let the Avro library handle supported types.
                    }

                    avroRecord.Add(field.Name, value);
                }
                else
                {
                    Console.WriteLine($"Warning: Property '{field.Name}' not found on POCO type '{pocoType.Name}'. Using default value.");
                }
            }
            return avroRecord;
        }
    }
}
