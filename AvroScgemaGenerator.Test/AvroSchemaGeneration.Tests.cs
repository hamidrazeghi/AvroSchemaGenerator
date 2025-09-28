using Xunit;
using AvroSchemaGeneration;
using System;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Linq;

using Avro.Generic;
using Avro;
using System.Collections;

namespace AvroSchemaGeneration.Tests
{
    public enum Status
    {
        Active,
        Inactive,
        Pending
    }

    public class Address
    {
        public string Street { get; set; }
        public int ZipCode { get; set; }
    }

    public class UserProfile
    {
        public long UserId { get; set; }
        public string Username { get; set; }
        public bool IsVerified { get; set; }
        public Status UserStatus { get; set; }
        public DateTime CreationDate { get; set; }
        public int? Age { get; set; } 
        public decimal AccountBalance { get; set; }
        public Address HomeAddress { get; set; } 
        public List<string> Tags { get; set; } 
    }
    // ----------------------------


    public class AvroSchemaGeneratorTests
    {
        [Fact]
        public void GenerateSchema_PrimitiveTypes_ShouldReturnCorrectSchema()
        {
            Assert.Equal("\"string\"", AvroSchemaGenerator.GenerateSchema(typeof(string)));
            Assert.Equal("\"int\"", AvroSchemaGenerator.GenerateSchema(typeof(int)));
            Assert.Equal("\"long\"", AvroSchemaGenerator.GenerateSchema(typeof(long)));
            Assert.Equal("\"boolean\"", AvroSchemaGenerator.GenerateSchema(typeof(bool)));
            Assert.Equal("\"double\"", AvroSchemaGenerator.GenerateSchema(typeof(double)));
            Assert.Equal("\"bytes\"", AvroSchemaGenerator.GenerateSchema(typeof(byte[])));
        }

        [Fact]
        public void GenerateSchema_DateTime_ShouldReturnLogicalType()
        {
            var expected = "{ \"type\": \"long\", \"logicalType\": \"timestamp-millis\" }";
            var actual = AvroSchemaGenerator.GenerateSchema(typeof(DateTime));
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void GenerateSchema_NullableValueType_ShouldReturnUnionWithNull()
        {
            var expected = "[\"null\", \"int\"]";
            var actual = AvroSchemaGenerator.GenerateSchema(typeof(int?));
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void GenerateSchema_Enum_ShouldReturnEnumSchema()
        {
            var actual = AvroSchemaGenerator.GenerateSchema(typeof(Status));
            Assert.Contains("\"type\": \"enum\"", actual);
            Assert.Contains("\"name\": \"Status\"", actual);
            Assert.Contains("\"symbols\": [\"Active\", \"Inactive\", \"Pending\"]", actual);
        }

        [Fact]
        public void GenerateSchema_ComplexType_ShouldReturnRecordSchema()
        {
            var actual = AvroSchemaGenerator.GenerateSchema(typeof(UserProfile));

            Assert.Contains("\"type\": \"record\"", actual);
            Assert.Contains("\"name\": \"UserProfile\"", actual);
            Assert.Contains("\"fields\": [", actual);

            Assert.Contains("{ \"name\": \"UserId\", \"type\": \"long\", \"default\": 0 }", actual);

            Assert.Contains("{ \"name\": \"Username\", \"type\": [\"null\", \"string\"], \"default\": null }", actual);

            Assert.Contains("{ \"name\": \"Age\", \"type\": [\"null\", \"int\"], \"default\": null }", actual);

            var addressFieldPattern = new Regex("\"name\": \"HomeAddress\", \"type\": \\[ *\"null\" *, *\\{ *\"type\": *\"record\" *, *\"name\": *\"Address\" *.*? *\\} *\\], *\"default\": *null *\\}");
            Assert.True(addressFieldPattern.IsMatch(actual), "HomeAddress field structure is incorrect.");

            var tagsFieldPattern = new Regex("\"name\": \"Tags\", \"type\": \\[ *\"null\" *, *\\{ *\"type\": *\"array\" *, *\"items\": *\"string\" *\\} *\\], *\"default\": *null *\\}");
            Assert.True(tagsFieldPattern.IsMatch(actual), "Tags field structure is incorrect.");
        }

        [Fact]
        public void ConvertToGenericRecord_ShouldMapAllFieldsCorrectly()
        {
            // Arrange
            var creationTime = new DateTime(2025, 1, 15, 10, 30, 0, DateTimeKind.Utc);
            var poco = new UserProfile
            {
                UserId = 12345,
                Username = "testuser",
                IsVerified = true,
                UserStatus = Status.Active,
                CreationDate = creationTime,
                Age = 35,
                AccountBalance = 12345.67m,
                HomeAddress = new Address { Street = "10 Downing St", ZipCode = 90210 },
                Tags = new List<string> { "premium", "beta" }
            };

            var schemaJson = AvroSchemaGenerator.GenerateSchema(typeof(UserProfile));
            var schema = Schema.Parse(schemaJson) as RecordSchema;
            Assert.NotNull(schema); 

            // Act
            var avroRecord = AvroSchemaGenerator.ConvertToGenericRecord(poco, schema);

            // Assert
            Assert.Equal(poco.UserId, avroRecord["UserId"]);
            Assert.Equal(poco.Username, avroRecord["Username"]);
            Assert.Equal(poco.IsVerified, avroRecord["IsVerified"]);

            Assert.Equal(poco.UserStatus.ToString(), avroRecord["UserStatus"]);

            Assert.Equal(creationTime, avroRecord["CreationDate"]);

            Assert.Equal(poco.Age, avroRecord["Age"]);

            
            Assert.Equal(poco.AccountBalance.ToString(), avroRecord["AccountBalance"]);

            var nestedRecord = (Address)avroRecord["HomeAddress"];
            Assert.NotNull(nestedRecord);
            Assert.Equal(poco.HomeAddress.Street, nestedRecord.Street);
            Assert.Equal(poco.HomeAddress.ZipCode, nestedRecord.ZipCode);

            // Array check
            var tagsList = (IList)avroRecord["Tags"];
            Assert.NotNull(tagsList);
            Assert.True(poco.Tags.SequenceEqual(tagsList.Cast<string>()));
        }
    }
}
