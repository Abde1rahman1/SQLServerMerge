# SQLServerMerge

A lightweight .NET library for performing SQL Server `MERGE` (upsert) operations using `SqlBulkCopy` and a fluent builder. No EF Core dependency required.

## Installation

```
dotnet add package SQLServerMerge
```

## Quick Start

```csharp
using Microsoft.Data.SqlClient;
using SQLServerMerge;

var products = new List<Product>
{
    new Product { Id = 1, Name = "Widget", Price = 9.99m },
    new Product { Id = 2, Name = "Gadget", Price = 19.99m }
};

using var connection = new SqlConnection("your-connection-string");
connection.Open();

// Synchronous — matches on Id, updates existing rows, inserts new ones
connection.ExecuteMerge(products, "Products", "Id");

// Asynchronous
await connection.ExecuteMergeAsync(products, "Products", "Id");
```

## How It Works

1. A temporary table (`#SourceTable`) is created with the same schema as the target table.
2. Data is bulk-copied into the temp table via `SqlBulkCopy`.
3. A `MERGE` statement is generated and executed — matching rows are updated, new rows are inserted.
4. The temp table is cleaned up in a `finally` block.

Generated SQL (for the Quick Start example above):

```sql
MERGE INTO [Products] AS Target
USING #SourceTable AS Source
ON Target.[Id] = Source.[Id]
WHEN MATCHED THEN
    UPDATE SET Target.[Name] = Source.[Name], Target.[Price] = Source.[Price]
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([Id], [Name], [Price])
    VALUES (Source.[Id], Source.[Name], Source.[Price])
;
```

## Features

Every feature below is available through `ExecuteMerge` / `ExecuteMergeAsync` extension methods on `SqlConnection`. All sync overloads have a matching async version that accepts an optional `CancellationToken`.

### Composite Key

When your table has a multi-column primary key, pass a `string[]` instead of a single key:

```csharp
// Sync
connection.ExecuteMerge(orderLines, "OrderLines", new[] { "OrderId", "ProductId" });

// Async
await connection.ExecuteMergeAsync(orderLines, "OrderLines", new[] { "OrderId", "ProductId" });
```

This generates an `ON` clause with `AND`:

```sql
ON Target.[OrderId] = Source.[OrderId] AND Target.[ProductId] = Source.[ProductId]
```

### `[Column]` Attribute Support

Properties decorated with `[Column]` are automatically mapped to their SQL column names. No extra configuration is needed:

```csharp
public class Product
{
    [Column("product_id")]
    public int ProductId { get; set; }

    [Column("product_name")]
    public string ProductName { get; set; } = string.Empty;

    public decimal Price { get; set; }  // no attribute → uses "Price"
}

// Just call ExecuteMerge — [Column] attributes are resolved automatically
connection.ExecuteMerge(products, "Products", "ProductId");
```

### EF Core Fluent API (DbContext)

If your column names are configured via EF Core `HasColumnName()` in `OnModelCreating`, pass your `DbContext` instance. The library reads the model at runtime via reflection — no EF Core package reference is required in the library itself:

```csharp
// OnModelCreating:
//   e.Property(p => p.ProductName).HasColumnName("product_name");

using var db = new AppDbContext();

// Single key
connection.ExecuteMerge(products, "Products", "Id", db);

// Composite key
connection.ExecuteMerge(orderLines, "OrderLines", new[] { "OrderId", "ProductId" }, db);

// Async
await connection.ExecuteMergeAsync(products, "Products", "Id", db);
await connection.ExecuteMergeAsync(orderLines, "OrderLines", new[] { "OrderId", "ProductId" }, db);
```

### Explicit Column Mappings (Dictionary)

Pass an `IDictionary<string, string>` mapping C# property names → SQL column names. This overrides both `[Column]` attributes and property names:

```csharp
var mappings = new Dictionary<string, string>
{
    { "Name", "product_name" },
    { "Price", "unit_price" }
};

// Single key
connection.ExecuteMerge(products, "Products", "Id", mappings);

// Composite key
connection.ExecuteMerge(orderLines, "OrderLines", new[] { "OrderId", "ProductId" }, mappings);

// Async
await connection.ExecuteMergeAsync(products, "Products", "Id", mappings);
await connection.ExecuteMergeAsync(orderLines, "OrderLines", new[] { "OrderId", "ProductId" }, mappings);
```

### Column Name Resolution Priority

| Priority | Source | Example |
|----------|--------|---------|
| 1 | Explicit dictionary / `WithColumnMapping()` | `{ "Name", "product_name" }` |
| 2 | `[Column]` attribute | `[Column("product_name")]` |
| 3 | C# property name | `public string Name` → `"Name"` |

When a `DbContext` is passed, EF Core `HasColumnName()` mappings are resolved at runtime and applied as priority 1 (explicit dictionary).

## MergeBuilder\<T\>

Use the fluent builder directly for full control over the generated SQL. `MergeBuilder<T>` is what `ExecuteMerge` uses internally — you can use it standalone when you need features like `Ignore` or `WithDeleteOrphans` that are not exposed through the extension methods.

### Ignore columns

Exclude properties from both `UPDATE SET` and `INSERT`:

```csharp
var sql = new MergeBuilder<Product>()
    .IntoTable("Products")
    .WithKey("Id")
    .Ignore("CreatedDate")
    .Ignore("InternalNotes")
    .Build();
```

### Delete orphan rows

Add `WHEN NOT MATCHED BY SOURCE THEN DELETE` for a full-sync merge that removes target rows not present in the source data:

```csharp
var sql = new MergeBuilder<Product>()
    .IntoTable("Products")
    .WithKey("Id")
    .WithDeleteOrphans()
    .Build();
```

### Explicit column mapping

Override the SQL column name for a specific C# property:

```csharp
var sql = new MergeBuilder<Product>()
    .IntoTable("Products")
    .WithKey("Id")
    .WithColumnMapping("Name", "product_name")
    .WithColumnMapping("Price", "unit_price")
    .Build();
```

### Composite key

```csharp
var sql = new MergeBuilder<OrderLine>()
    .IntoTable("OrderLines")
    .WithCompositeKey("OrderId", "ProductId")
    .Build();
```

### Full chain example

```csharp
var sql = new MergeBuilder<OrderLine>()
    .IntoTable("OrderLines")
    .WithCompositeKey("OrderId", "ProductId")
    .Ignore("CreatedDate")
    .WithColumnMapping("Quantity", "qty")
    .WithDeleteOrphans()
    .Build();
```

### Builder API Reference

| Method | Description |
|--------|-------------|
| `.IntoTable(string)` | Target table name |
| `.WithKey(string)` | Single merge key (C# property name) |
| `.WithCompositeKey(params string[])` | Multi-column merge key |
| `.Ignore(string)` | Exclude a property from UPDATE and INSERT |
| `.WithColumnMapping(string, string)` | Map a C# property to a SQL column name |
| `.WithDeleteOrphans()` | Add `WHEN NOT MATCHED BY SOURCE THEN DELETE` |
| `.Build()` | Returns the generated SQL string |

## Requirements

- .NET Standard 2.1+
- Microsoft.Data.SqlClient

## License

MIT
