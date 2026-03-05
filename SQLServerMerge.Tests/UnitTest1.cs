using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Linq;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using SQLServerMerge;

namespace SQLServerMerge.Tests;

public class TestEntity
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
}

public class NullableEntity
{
    public int Id { get; set; }
    public string? Description { get; set; }
    public int? Quantity { get; set; }
    public DateTime? UpdatedAt { get; set; }
}

public class SinglePropertyEntity
{
    public int Id { get; set; }
}

public class CompositeKeyEntity
{
    public int TenantId { get; set; }
    public int ProductId { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
}

public class ColumnAttributeEntity
{
    [Column("product_id")]
    public int ProductId { get; set; }

    [Column("product_name")]
    public string ProductName { get; set; } = string.Empty;

    public decimal Price { get; set; }
}

public class PartialColumnAttributeEntity
{
    [Column("id")]
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;
}

#region MergeBuilder Tests

public class MergeBuilder_Build_Tests
{
    [Fact]
    public void Build_WithoutTableName_ThrowsInvalidOperationException()
    {
        var builder = new MergeBuilder<TestEntity>()
            .WithKey("Id");

        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains("Table name", ex.Message);
    }

    [Fact]
    public void Build_WithoutKey_ThrowsInvalidOperationException()
    {
        var builder = new MergeBuilder<TestEntity>()
            .IntoTable("Products");

        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains("Key property", ex.Message);
    }

    [Fact]
    public void Build_WithEmptyTableName_ThrowsInvalidOperationException()
    {
        var builder = new MergeBuilder<TestEntity>()
            .IntoTable("")
            .WithKey("Id");

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Fact]
    public void Build_WithEmptyKey_ThrowsArgumentException()
    {
        var builder = new MergeBuilder<TestEntity>()
            .IntoTable("Products");

        Assert.Throws<ArgumentException>(() => builder.WithKey(""));
    }

    [Fact]
    public void Build_NeitherTableNameNorKey_ThrowsInvalidOperationException()
    {
        var builder = new MergeBuilder<TestEntity>();

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Fact]
    public void Build_ValidConfiguration_ContainsMergeInto()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .Build();

        Assert.Contains("MERGE INTO [Products] AS Target", sql);
    }

    [Fact]
    public void Build_ValidConfiguration_ContainsUsingSourceTable()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .Build();

        Assert.Contains("USING #SourceTable AS Source", sql);
    }

    [Fact]
    public void Build_ValidConfiguration_ContainsOnClauseWithKey()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .Build();

        Assert.Contains("ON Target.[Id] = Source.[Id]", sql);
    }

    [Fact]
    public void Build_ValidConfiguration_ContainsWhenMatchedUpdate()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .Build();

        Assert.Contains("WHEN MATCHED THEN", sql);
        Assert.Contains("UPDATE SET", sql);
    }

    [Fact]
    public void Build_ValidConfiguration_ContainsWhenNotMatchedInsert()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .Build();

        Assert.Contains("WHEN NOT MATCHED BY TARGET THEN", sql);
        Assert.Contains("INSERT (", sql);
        Assert.Contains("VALUES (", sql);
    }

    [Fact]
    public void Build_ValidConfiguration_UpdateSetExcludesKeyColumn()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .Build();

        // Extract only the UPDATE SET line to verify the key is not updated
        var lines = sql.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
        var updateSetLine = lines.First(l => l.TrimStart().StartsWith("UPDATE SET"));
        Assert.Contains("Target.[Name] = Source.[Name]", updateSetLine);
        Assert.Contains("Target.[Price] = Source.[Price]", updateSetLine);
        Assert.DoesNotContain("Target.[Id] = Source.[Id]", updateSetLine);
    }

    [Fact]
    public void Build_ValidConfiguration_InsertIncludesAllColumns()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .Build();

        Assert.Contains("INSERT ([Id], [Name], [Price])", sql);
        Assert.Contains("VALUES (Source.[Id], Source.[Name], Source.[Price])", sql);
    }

    [Fact]
    public void Build_ValidConfiguration_EndsWithSemicolon()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .Build();

        Assert.EndsWith(";", sql.TrimEnd());
    }

    [Fact]
    public void Build_WithIgnoredColumn_ExcludesFromUpdateAndInsert()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .Ignore("Price")
            .Build();

        Assert.DoesNotContain("[Price]", sql);
        Assert.Contains("Target.[Name] = Source.[Name]", sql);
    }

    [Fact]
    public void Build_WithMultipleIgnoredColumns_ExcludesAllFromSql()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .Ignore("Name")
            .Ignore("Price")
            .Build();

        Assert.DoesNotContain("[Name]", sql);
        Assert.DoesNotContain("[Price]", sql);
    }

    [Fact]
    public void Build_SinglePropertyEntity_OmitsWhenMatchedClause()
    {
        var sql = new MergeBuilder<SinglePropertyEntity>()
            .IntoTable("Items")
            .WithKey("Id")
            .Build();

        Assert.Contains("MERGE INTO [Items] AS Target", sql);
        Assert.DoesNotContain("WHEN MATCHED THEN", sql);
        Assert.DoesNotContain("UPDATE SET", sql);
        Assert.Contains("INSERT ([Id])", sql);
        Assert.Contains("VALUES (Source.[Id])", sql);
    }

    [Fact]
    public void Build_TableNameWithSpaces_BracketQuoted()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("My Table")
            .WithKey("Id")
            .Build();

        Assert.Contains("MERGE INTO [My Table] AS Target", sql);
    }
}

public class MergeBuilder_DeleteOrphans_Tests
{
    [Fact]
    public void Build_WithDeleteOrphans_ContainsDeleteClause()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .WithDeleteOrphans()
            .Build();

        Assert.Contains("WHEN NOT MATCHED BY SOURCE THEN", sql);
        Assert.Contains("DELETE", sql);
    }

    [Fact]
    public void Build_WithoutDeleteOrphans_DoesNotContainDeleteClause()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .Build();

        Assert.DoesNotContain("WHEN NOT MATCHED BY SOURCE THEN", sql);
    }

    [Fact]
    public void Build_WithDeleteOrphans_StillContainsInsertAndUpdate()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .WithDeleteOrphans()
            .Build();

        Assert.Contains("WHEN MATCHED THEN", sql);
        Assert.Contains("UPDATE SET", sql);
        Assert.Contains("WHEN NOT MATCHED BY TARGET THEN", sql);
        Assert.Contains("INSERT (", sql);
    }

    [Fact]
    public void Build_WithDeleteOrphans_EndsWithSemicolon()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .WithDeleteOrphans()
            .Build();

        Assert.EndsWith(";", sql.TrimEnd());
    }

    [Fact]
    public void Build_WithDeleteOrphans_SinglePropertyEntity()
    {
        var sql = new MergeBuilder<SinglePropertyEntity>()
            .IntoTable("Items")
            .WithKey("Id")
            .WithDeleteOrphans()
            .Build();

        Assert.DoesNotContain("WHEN MATCHED THEN", sql);
        Assert.Contains("WHEN NOT MATCHED BY TARGET THEN", sql);
        Assert.Contains("WHEN NOT MATCHED BY SOURCE THEN", sql);
        Assert.Contains("DELETE", sql);
    }

    [Fact]
    public void Build_WithDeleteOrphans_DeleteClauseAppearsAfterInsert()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .WithDeleteOrphans()
            .Build();

        var insertPos = sql.IndexOf("WHEN NOT MATCHED BY TARGET THEN");
        var deletePos = sql.IndexOf("WHEN NOT MATCHED BY SOURCE THEN");
        Assert.True(deletePos > insertPos);
    }
}

public class MergeBuilder_CompositeKey_Tests
{
    [Fact]
    public void Build_CompositeKey_ContainsAndInOnClause()
    {
        var sql = new MergeBuilder<CompositeKeyEntity>()
            .IntoTable("Products")
            .WithCompositeKey("TenantId", "ProductId")
            .Build();

        Assert.Contains("Target.[TenantId] = Source.[TenantId] AND Target.[ProductId] = Source.[ProductId]", sql);
    }

    [Fact]
    public void Build_CompositeKey_ExcludesAllKeysFromUpdate()
    {
        var sql = new MergeBuilder<CompositeKeyEntity>()
            .IntoTable("Products")
            .WithCompositeKey("TenantId", "ProductId")
            .Build();

        var lines = sql.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
        var updateSetLine = lines.First(l => l.TrimStart().StartsWith("UPDATE SET"));
        Assert.DoesNotContain("Target.[TenantId]", updateSetLine);
        Assert.DoesNotContain("Target.[ProductId]", updateSetLine);
        Assert.Contains("Target.[Name] = Source.[Name]", updateSetLine);
        Assert.Contains("Target.[Price] = Source.[Price]", updateSetLine);
    }

    [Fact]
    public void Build_CompositeKey_InsertIncludesAllColumns()
    {
        var sql = new MergeBuilder<CompositeKeyEntity>()
            .IntoTable("Products")
            .WithCompositeKey("TenantId", "ProductId")
            .Build();

        Assert.Contains("[TenantId]", sql);
        Assert.Contains("[ProductId]", sql);
        Assert.Contains("[Name]", sql);
        Assert.Contains("[Price]", sql);
    }

    [Fact]
    public void Build_CompositeKey_WithColumnMapping()
    {
        var sql = new MergeBuilder<CompositeKeyEntity>()
            .IntoTable("Products")
            .WithCompositeKey("TenantId", "ProductId")
            .WithColumnMapping("TenantId", "tenant_id")
            .WithColumnMapping("ProductId", "product_id")
            .Build();

        Assert.Contains("Target.[tenant_id] = Source.[tenant_id] AND Target.[product_id] = Source.[product_id]", sql);
    }

    [Fact]
    public void Build_CompositeKey_WithDeleteOrphans()
    {
        var sql = new MergeBuilder<CompositeKeyEntity>()
            .IntoTable("Products")
            .WithCompositeKey("TenantId", "ProductId")
            .WithDeleteOrphans()
            .Build();

        Assert.Contains("WHEN NOT MATCHED BY SOURCE THEN", sql);
        Assert.Contains("DELETE", sql);
    }

    [Fact]
    public void Build_CompositeKey_SingleKeyFallback()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithCompositeKey("Id")
            .Build();

        Assert.Contains("ON Target.[Id] = Source.[Id]", sql);
        Assert.DoesNotContain("AND", sql);
    }

    [Fact]
    public void WithCompositeKey_EmptyArray_ThrowsArgumentException()
    {
        var builder = new MergeBuilder<TestEntity>()
            .IntoTable("Products");

        Assert.Throws<ArgumentException>(() => builder.WithCompositeKey());
    }

    [Fact]
    public void WithCompositeKey_NullArray_ThrowsArgumentNullException()
    {
        var builder = new MergeBuilder<TestEntity>()
            .IntoTable("Products");

        Assert.Throws<ArgumentNullException>(() => builder.WithCompositeKey(null!));
    }

    [Fact]
    public void Build_WithoutKey_ThrowsInvalidOperationException()
    {
        var builder = new MergeBuilder<TestEntity>()
            .IntoTable("Products");

        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains("Key property", ex.Message);
    }

    [Fact]
    public void WithCompositeKey_OverridesWithKey()
    {
        var sql = new MergeBuilder<CompositeKeyEntity>()
            .IntoTable("Products")
            .WithKey("TenantId")
            .WithCompositeKey("TenantId", "ProductId")
            .Build();

        Assert.Contains("AND", sql);
    }

    [Fact]
    public void WithKey_OverridesWithCompositeKey()
    {
        var sql = new MergeBuilder<CompositeKeyEntity>()
            .IntoTable("Products")
            .WithCompositeKey("TenantId", "ProductId")
            .WithKey("TenantId")
            .Build();

        Assert.DoesNotContain("AND", sql);
    }
}

public class MergeBuilder_FluentApi_Tests
{
    [Fact]
    public void IntoTable_ReturnsSameBuilder()
    {
        var builder = new MergeBuilder<TestEntity>();

        var result = builder.IntoTable("Products");

        Assert.Same(builder, result);
    }

    [Fact]
    public void WithKey_ReturnsSameBuilder()
    {
        var builder = new MergeBuilder<TestEntity>();

        var result = builder.WithKey("Id");

        Assert.Same(builder, result);
    }

    [Fact]
    public void Ignore_ReturnsSameBuilder()
    {
        var builder = new MergeBuilder<TestEntity>();

        var result = builder.Ignore("Name");

        Assert.Same(builder, result);
    }

    [Fact]
    public void WithColumnMapping_ReturnsSameBuilder()
    {
        var builder = new MergeBuilder<TestEntity>();

        var result = builder.WithColumnMapping("Name", "product_name");

        Assert.Same(builder, result);
    }

    [Fact]
    public void WithDeleteOrphans_ReturnsSameBuilder()
    {
        var builder = new MergeBuilder<TestEntity>();

        var result = builder.WithDeleteOrphans();

        Assert.Same(builder, result);
    }

    [Fact]
    public void WithCompositeKey_ReturnsSameBuilder()
    {
        var builder = new MergeBuilder<CompositeKeyEntity>();

        var result = builder.WithCompositeKey("TenantId", "ProductId");

        Assert.Same(builder, result);
    }

    [Fact]
    public void FluentChain_AllMethodsChainable()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .Ignore("Price")
            .Build();

        Assert.NotNull(sql);
        Assert.NotEmpty(sql);
    }

    [Fact]
    public void Build_CanBeCalledMultipleTimes()
    {
        var builder = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id");

        var sql1 = builder.Build();
        var sql2 = builder.Build();

        Assert.Equal(sql1, sql2);
    }
}

public class MergeBuilder_NullArgument_Tests
{
    [Fact]
    public void IntoTable_NullTableName_ThrowsArgumentNullException()
    {
        var builder = new MergeBuilder<TestEntity>();

        Assert.Throws<ArgumentNullException>(() => builder.IntoTable(null!));
    }

    [Fact]
    public void WithKey_NullKey_ThrowsArgumentNullException()
    {
        var builder = new MergeBuilder<TestEntity>();

        Assert.Throws<ArgumentNullException>(() => builder.WithKey(null!));
    }

    [Fact]
    public void Ignore_NullPropertyName_ThrowsArgumentNullException()
    {
        var builder = new MergeBuilder<TestEntity>();

        Assert.Throws<ArgumentNullException>(() => builder.Ignore(null!));
    }

    [Fact]
    public void WithColumnMapping_NullPropertyName_ThrowsArgumentNullException()
    {
        var builder = new MergeBuilder<TestEntity>();

        Assert.Throws<ArgumentNullException>(() => builder.WithColumnMapping(null!, "col"));
    }

    [Fact]
    public void WithColumnMapping_NullColumnName_ThrowsArgumentNullException()
    {
        var builder = new MergeBuilder<TestEntity>();

        Assert.Throws<ArgumentNullException>(() => builder.WithColumnMapping("Name", null!));
    }
}

#endregion

#region Column Mapping Tests

public class MergeBuilder_ColumnAttribute_Tests
{
    [Fact]
    public void Build_WithColumnAttribute_UsesAttributeNameInOnClause()
    {
        var sql = new MergeBuilder<ColumnAttributeEntity>()
            .IntoTable("Products")
            .WithKey("ProductId")
            .Build();

        Assert.Contains("ON Target.[product_id] = Source.[product_id]", sql);
    }

    [Fact]
    public void Build_WithColumnAttribute_UsesAttributeNameInUpdateSet()
    {
        var sql = new MergeBuilder<ColumnAttributeEntity>()
            .IntoTable("Products")
            .WithKey("ProductId")
            .Build();

        Assert.Contains("Target.[product_name] = Source.[product_name]", sql);
    }

    [Fact]
    public void Build_WithColumnAttribute_UsesAttributeNameInInsert()
    {
        var sql = new MergeBuilder<ColumnAttributeEntity>()
            .IntoTable("Products")
            .WithKey("ProductId")
            .Build();

        Assert.Contains("[product_id]", sql);
        Assert.Contains("[product_name]", sql);
        Assert.Contains("Source.[product_id]", sql);
        Assert.Contains("Source.[product_name]", sql);
    }

    [Fact]
    public void Build_WithColumnAttribute_UnmappedPropertyUsesPropertyName()
    {
        var sql = new MergeBuilder<ColumnAttributeEntity>()
            .IntoTable("Products")
            .WithKey("ProductId")
            .Build();

        Assert.Contains("[Price]", sql);
        Assert.Contains("Target.[Price] = Source.[Price]", sql);
    }

    [Fact]
    public void Build_PartialColumnAttribute_MixesAttributeAndPropertyNames()
    {
        var sql = new MergeBuilder<PartialColumnAttributeEntity>()
            .IntoTable("Items")
            .WithKey("Id")
            .Build();

        Assert.Contains("ON Target.[id] = Source.[id]", sql);
        Assert.Contains("Target.[Name] = Source.[Name]", sql);
    }

    [Fact]
    public void Build_WithColumnAttribute_IgnoreStillUsesCSharpPropertyName()
    {
        var sql = new MergeBuilder<ColumnAttributeEntity>()
            .IntoTable("Products")
            .WithKey("ProductId")
            .Ignore("ProductName")
            .Build();

        Assert.DoesNotContain("product_name", sql);
        Assert.Contains("[Price]", sql);
    }

    [Fact]
    public void Build_WithColumnAttribute_DoesNotContainCSharpPropertyNames()
    {
        var sql = new MergeBuilder<ColumnAttributeEntity>()
            .IntoTable("Products")
            .WithKey("ProductId")
            .Build();

        Assert.DoesNotContain("[ProductId]", sql);
        Assert.DoesNotContain("[ProductName]", sql);
    }
}

public class MergeBuilder_WithColumnMapping_Tests
{
    [Fact]
    public void Build_WithColumnMapping_UsesCustomNameInSql()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .WithColumnMapping("Name", "product_name")
            .Build();

        Assert.Contains("Target.[product_name] = Source.[product_name]", sql);
        Assert.DoesNotContain("[Name]", sql);
    }

    [Fact]
    public void Build_WithColumnMapping_OnKeyColumn()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .WithColumnMapping("Id", "product_id")
            .Build();

        Assert.Contains("ON Target.[product_id] = Source.[product_id]", sql);
    }

    [Fact]
    public void Build_WithColumnMapping_OverridesColumnAttribute()
    {
        var sql = new MergeBuilder<ColumnAttributeEntity>()
            .IntoTable("Products")
            .WithKey("ProductId")
            .WithColumnMapping("ProductName", "custom_name")
            .Build();

        Assert.Contains("[custom_name]", sql);
        Assert.DoesNotContain("[product_name]", sql);
    }

    [Fact]
    public void Build_WithMultipleColumnMappings()
    {
        var sql = new MergeBuilder<TestEntity>()
            .IntoTable("Products")
            .WithKey("Id")
            .WithColumnMapping("Id", "product_id")
            .WithColumnMapping("Name", "product_name")
            .WithColumnMapping("Price", "unit_price")
            .Build();

        Assert.Contains("ON Target.[product_id] = Source.[product_id]", sql);
        Assert.Contains("Target.[product_name] = Source.[product_name]", sql);
        Assert.Contains("Target.[unit_price] = Source.[unit_price]", sql);
        Assert.Contains("INSERT ([product_id], [product_name], [unit_price])", sql);
    }
}

public class ToDataTable_ColumnAttribute_Tests
{
    [Fact]
    public void ToDataTable_WithColumnAttribute_UsesAttributeNamesAsColumns()
    {
        var data = new[] { new ColumnAttributeEntity { ProductId = 1, ProductName = "Widget", Price = 9.99m } };

        var table = MergeExtensions.ToDataTable(data);

        Assert.Contains("product_id", table.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
        Assert.Contains("product_name", table.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
        Assert.Contains("Price", table.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
    }

    [Fact]
    public void ToDataTable_WithColumnAttribute_StoresValuesUnderMappedNames()
    {
        var data = new[] { new ColumnAttributeEntity { ProductId = 42, ProductName = "Gadget", Price = 19.99m } };

        var table = MergeExtensions.ToDataTable(data);

        Assert.Equal(42, table.Rows[0]["product_id"]);
        Assert.Equal("Gadget", table.Rows[0]["product_name"]);
        Assert.Equal(19.99m, table.Rows[0]["Price"]);
    }

    [Fact]
    public void ToDataTable_WithColumnAttribute_DoesNotContainCSharpPropertyNames()
    {
        var data = new[] { new ColumnAttributeEntity { ProductId = 1, ProductName = "Test", Price = 1m } };

        var table = MergeExtensions.ToDataTable(data);

        Assert.DoesNotContain("ProductId", table.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
        Assert.DoesNotContain("ProductName", table.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
    }

    [Fact]
    public void ToDataTable_PartialColumnAttribute_MixesNames()
    {
        var data = new[] { new PartialColumnAttributeEntity { Id = 1, Name = "Test" } };

        var table = MergeExtensions.ToDataTable(data);

        Assert.Contains("id", table.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
        Assert.Contains("Name", table.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
    }
}

#endregion

#region ToDataTable Tests

public class ToDataTable_Tests
{
    [Fact]
    public void ToDataTable_EmptyCollection_ReturnsTableWithColumnsOnly()
    {
        var data = Enumerable.Empty<TestEntity>();

        var table = MergeExtensions.ToDataTable(data);

        Assert.Equal(3, table.Columns.Count);
        Assert.Equal(0, table.Rows.Count);
        Assert.Contains("Id", table.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
        Assert.Contains("Name", table.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
        Assert.Contains("Price", table.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
    }

    [Fact]
    public void ToDataTable_SingleItem_ReturnsOneRow()
    {
        var data = new[] { new TestEntity { Id = 1, Name = "Widget", Price = 9.99m } };

        var table = MergeExtensions.ToDataTable(data);

        Assert.Equal(1, table.Rows.Count);
        Assert.Equal(1, table.Rows[0]["Id"]);
        Assert.Equal("Widget", table.Rows[0]["Name"]);
        Assert.Equal(9.99m, table.Rows[0]["Price"]);
    }

    [Fact]
    public void ToDataTable_MultipleItems_ReturnsAllRows()
    {
        var data = new[]
        {
            new TestEntity { Id = 1, Name = "A", Price = 1m },
            new TestEntity { Id = 2, Name = "B", Price = 2m },
            new TestEntity { Id = 3, Name = "C", Price = 3m }
        };

        var table = MergeExtensions.ToDataTable(data);

        Assert.Equal(3, table.Rows.Count);
    }

    [Fact]
    public void ToDataTable_ColumnTypesMatchProperties()
    {
        var data = Enumerable.Empty<TestEntity>();

        var table = MergeExtensions.ToDataTable(data);

        Assert.Equal(typeof(int), table.Columns["Id"]!.DataType);
        Assert.Equal(typeof(string), table.Columns["Name"]!.DataType);
        Assert.Equal(typeof(decimal), table.Columns["Price"]!.DataType);
    }

    [Fact]
    public void ToDataTable_NullablePropertyTypes_UsesUnderlyingType()
    {
        var data = Enumerable.Empty<NullableEntity>();

        var table = MergeExtensions.ToDataTable(data);

        Assert.Equal(typeof(int), table.Columns["Id"]!.DataType);
        Assert.Equal(typeof(string), table.Columns["Description"]!.DataType);
        Assert.Equal(typeof(int), table.Columns["Quantity"]!.DataType);
        Assert.Equal(typeof(DateTime), table.Columns["UpdatedAt"]!.DataType);
    }

    [Fact]
    public void ToDataTable_NullValues_StoredAsDBNull()
    {
        var data = new[] { new NullableEntity { Id = 1, Description = null, Quantity = null, UpdatedAt = null } };

        var table = MergeExtensions.ToDataTable(data);

        Assert.Equal(1, table.Rows[0]["Id"]);
        Assert.Equal(DBNull.Value, table.Rows[0]["Description"]);
        Assert.Equal(DBNull.Value, table.Rows[0]["Quantity"]);
        Assert.Equal(DBNull.Value, table.Rows[0]["UpdatedAt"]);
    }

    [Fact]
    public void ToDataTable_NonNullNullableValues_StoredCorrectly()
    {
        var date = new DateTime(2024, 1, 15);
        var data = new[] { new NullableEntity { Id = 1, Description = "Test", Quantity = 5, UpdatedAt = date } };

        var table = MergeExtensions.ToDataTable(data);

        Assert.Equal("Test", table.Rows[0]["Description"]);
        Assert.Equal(5, table.Rows[0]["Quantity"]);
        Assert.Equal(date, table.Rows[0]["UpdatedAt"]);
    }

    [Fact]
    public void ToDataTable_PreservesRowOrder()
    {
        var data = new[]
        {
            new TestEntity { Id = 3, Name = "Third", Price = 30m },
            new TestEntity { Id = 1, Name = "First", Price = 10m },
            new TestEntity { Id = 2, Name = "Second", Price = 20m }
        };

        var table = MergeExtensions.ToDataTable(data);

        Assert.Equal(3, table.Rows[0]["Id"]);
        Assert.Equal(1, table.Rows[1]["Id"]);
        Assert.Equal(2, table.Rows[2]["Id"]);
    }
}

#endregion

#region ToDataTable with ColumnMappings Tests

public class ToDataTable_ColumnMappings_Tests
{
    [Fact]
    public void ToDataTable_WithColumnMappings_UsesCustomColumnNames()
    {
        var data = new[] { new TestEntity { Id = 1, Name = "Widget", Price = 9.99m } };
        var mappings = new Dictionary<string, string>
        {
            { "Name", "product_name" },
            { "Price", "unit_price" }
        };

        var table = MergeExtensions.ToDataTable(data, mappings);

        var columnNames = table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
        Assert.Contains("Id", columnNames);
        Assert.Contains("product_name", columnNames);
        Assert.Contains("unit_price", columnNames);
        Assert.DoesNotContain("Name", columnNames);
        Assert.DoesNotContain("Price", columnNames);
    }

    [Fact]
    public void ToDataTable_WithColumnMappings_StoresValuesUnderMappedNames()
    {
        var data = new[] { new TestEntity { Id = 1, Name = "Widget", Price = 9.99m } };
        var mappings = new Dictionary<string, string>
        {
            { "Name", "product_name" },
            { "Price", "unit_price" }
        };

        var table = MergeExtensions.ToDataTable(data, mappings);

        Assert.Equal(1, table.Rows[0]["Id"]);
        Assert.Equal("Widget", table.Rows[0]["product_name"]);
        Assert.Equal(9.99m, table.Rows[0]["unit_price"]);
    }

    [Fact]
    public void ToDataTable_WithColumnMappings_OverridesColumnAttribute()
    {
        var data = new[] { new ColumnAttributeEntity { ProductId = 1, ProductName = "Test", Price = 5m } };
        var mappings = new Dictionary<string, string>
        {
            { "ProductName", "custom_name" }
        };

        var table = MergeExtensions.ToDataTable(data, mappings);

        var columnNames = table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
        Assert.Contains("custom_name", columnNames);
        Assert.DoesNotContain("product_name", columnNames);
        Assert.DoesNotContain("ProductName", columnNames);
        // Non-overridden [Column] attribute still works
        Assert.Contains("product_id", columnNames);
    }

    [Fact]
    public void ToDataTable_WithNullColumnMappings_FallsBackToDefault()
    {
        var data = new[] { new ColumnAttributeEntity { ProductId = 1, ProductName = "Test", Price = 5m } };

        var table = MergeExtensions.ToDataTable(data, null);

        var columnNames = table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
        Assert.Contains("product_id", columnNames);
        Assert.Contains("product_name", columnNames);
        Assert.Contains("Price", columnNames);
    }

    [Fact]
    public void ToDataTable_WithEmptyColumnMappings_FallsBackToDefault()
    {
        var data = new[] { new TestEntity { Id = 1, Name = "Widget", Price = 9.99m } };

        var table = MergeExtensions.ToDataTable(data, new Dictionary<string, string>());

        var columnNames = table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
        Assert.Contains("Id", columnNames);
        Assert.Contains("Name", columnNames);
        Assert.Contains("Price", columnNames);
    }

    [Fact]
    public void ToDataTable_WithPartialMappings_MixesMappedAndDefault()
    {
        var data = new[] { new TestEntity { Id = 1, Name = "Widget", Price = 9.99m } };
        var mappings = new Dictionary<string, string>
        {
            { "Name", "product_name" }
        };

        var table = MergeExtensions.ToDataTable(data, mappings);

        var columnNames = table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
        Assert.Contains("Id", columnNames);
        Assert.Contains("product_name", columnNames);
        Assert.Contains("Price", columnNames);
    }
}

#endregion

#region EfCoreMappingResolver Tests

public class FluentApiProduct
{
    public int Id { get; set; }
    public string ProductName { get; set; } = string.Empty;
    public decimal UnitPrice { get; set; }
}

public class NoMappingEntity
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class UnmappedEntity
{
    public int Id { get; set; }
}

public class TestDbContext : DbContext
{
    public DbSet<FluentApiProduct> Products { get; set; } = null!;
    public DbSet<NoMappingEntity> NoMappings { get; set; } = null!;

    private readonly SqliteConnection _connection;

    public TestDbContext()
    {
        _connection = new SqliteConnection("DataSource=:memory:");
        _connection.Open();
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseSqlite(_connection);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FluentApiProduct>(e =>
        {
            e.HasKey(p => p.Id);
            e.Property(p => p.Id).HasColumnName("product_id");
            e.Property(p => p.ProductName).HasColumnName("product_name");
            e.Property(p => p.UnitPrice).HasColumnName("unit_price");
        });

        modelBuilder.Entity<NoMappingEntity>(e =>
        {
            e.HasKey(p => p.Id);
        });
    }

    public override void Dispose()
    {
        _connection.Dispose();
        base.Dispose();
    }
}

public class EfCoreMappingResolver_Tests
{
    [Fact]
    public void ValidateDbContext_NullThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => EfCoreMappingResolver.ValidateDbContext(null!));
    }

    [Fact]
    public void ValidateDbContext_NonDbContextThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => EfCoreMappingResolver.ValidateDbContext("not a DbContext"));
    }

    [Fact]
    public void ValidateDbContext_RealDbContextDoesNotThrow()
    {
        using var db = new TestDbContext();

        EfCoreMappingResolver.ValidateDbContext(db);
    }

    [Fact]
    public void TryGetColumnMappings_FluentApiMappings_ReturnsAllColumns()
    {
        using var db = new TestDbContext();

        var mappings = EfCoreMappingResolver.TryGetColumnMappings(db, typeof(FluentApiProduct));

        Assert.NotNull(mappings);
        Assert.Equal("product_id", mappings!["Id"]);
        Assert.Equal("product_name", mappings["ProductName"]);
        Assert.Equal("unit_price", mappings["UnitPrice"]);
    }

    [Fact]
    public void TryGetColumnMappings_NoFluentApiMappings_ReturnsPropertyNames()
    {
        using var db = new TestDbContext();

        var mappings = EfCoreMappingResolver.TryGetColumnMappings(db, typeof(NoMappingEntity));

        Assert.NotNull(mappings);
        Assert.Equal("Id", mappings!["Id"]);
        Assert.Equal("Name", mappings["Name"]);
    }

    [Fact]
    public void TryGetColumnMappings_UnregisteredEntity_ReturnsNull()
    {
        using var db = new TestDbContext();

        var mappings = EfCoreMappingResolver.TryGetColumnMappings(db, typeof(UnmappedEntity));

        Assert.Null(mappings);
    }

    [Fact]
    public void ToDataTable_WithEfCoreMappings_UsesFluentApiColumnNames()
    {
        using var db = new TestDbContext();
        var data = new[] { new FluentApiProduct { Id = 1, ProductName = "Widget", UnitPrice = 9.99m } };
        var mappings = EfCoreMappingResolver.TryGetColumnMappings(db, typeof(FluentApiProduct));

        var table = MergeExtensions.ToDataTable(data, mappings);

        var columnNames = table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
        Assert.Contains("product_id", columnNames);
        Assert.Contains("product_name", columnNames);
        Assert.Contains("unit_price", columnNames);
        Assert.DoesNotContain("Id", columnNames);
        Assert.DoesNotContain("ProductName", columnNames);
        Assert.DoesNotContain("UnitPrice", columnNames);
    }

    [Fact]
    public void MergeBuilder_WithEfCoreMappings_GeneratesCorrectSql()
    {
        using var db = new TestDbContext();
        var mappings = EfCoreMappingResolver.TryGetColumnMappings(db, typeof(FluentApiProduct));

        var builder = new MergeBuilder<FluentApiProduct>()
            .IntoTable("Products")
            .WithKey("Id");

        foreach (var kvp in mappings!)
            builder.WithColumnMapping(kvp.Key, kvp.Value);

        var sql = builder.Build();

        Assert.Contains("ON Target.[product_id] = Source.[product_id]", sql);
        Assert.Contains("Target.[product_name] = Source.[product_name]", sql);
        Assert.Contains("Target.[unit_price] = Source.[unit_price]", sql);
        Assert.DoesNotContain("[ProductName]", sql);
        Assert.DoesNotContain("[UnitPrice]", sql);
    }
}

#endregion