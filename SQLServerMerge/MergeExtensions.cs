using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace SQLServerMerge
{
    /// <summary>
    /// Extension methods for executing SQL Server MERGE operations via <see cref="SqlConnection"/>.
    /// </summary>
    public static class MergeExtensions
    {
        /// <summary>
        /// Executes a MERGE statement against the specified table using the provided data.
        /// Column names are resolved from [Column] attributes or property names.
        /// </summary>
        /// <typeparam name="T">The model type whose properties map to table columns.</typeparam>
        /// <param name="connection">An open <see cref="SqlConnection"/>.</param>
        /// <param name="data">The source data to merge.</param>
        /// <param name="tableName">The target table name.</param>
        /// <param name="key">The C# property name used as the merge key.</param>
        public static void ExecuteMerge<T>(this SqlConnection connection, IEnumerable<T> data, string tableName, string key) where T : class
        {
            ExecuteMerge(connection, data, tableName, key, (IDictionary<string, string>?)null);
        }

        /// <summary>
        /// Executes a MERGE statement using EF Core metadata from the provided DbContext instance.
        /// </summary>
        /// <typeparam name="T">The model type whose properties map to table columns.</typeparam>
        /// <param name="connection">An open <see cref="SqlConnection"/>.</param>
        /// <param name="data">The source data to merge.</param>
        /// <param name="tableName">The target table name.</param>
        /// <param name="key">The C# property name used as the merge key.</param>
        /// <param name="dbContext">An EF Core <c>DbContext</c> instance used to resolve Fluent API mappings.</param>
        public static void ExecuteMerge<T>(this SqlConnection connection, IEnumerable<T> data, string tableName, string key, object dbContext) where T : class
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentException("Key cannot be null or empty.", nameof(key));
            EfCoreMappingResolver.ValidateDbContext(dbContext);
            var mappings = EfCoreMappingResolver.TryGetColumnMappings(dbContext, typeof(T));
            ExecuteMerge(connection, data, tableName, new[] { key }, mappings);
        }

        /// <summary>
        /// Executes a MERGE statement against the specified table using the provided data,
        /// with explicit column mappings for properties whose SQL column names differ from C# property names
        /// (e.g. when configured via EF Core's <c>OnModelCreating</c>).
        /// </summary>
        /// <typeparam name="T">The model type whose properties map to table columns.</typeparam>
        /// <param name="connection">An open <see cref="SqlConnection"/>.</param>
        /// <param name="data">The source data to merge.</param>
        /// <param name="tableName">The target table name.</param>
        /// <param name="key">The C# property name used as the merge key.</param>
        /// <param name="columnMappings">
        /// A dictionary mapping C# property names to SQL column names.
        /// Overrides [Column] attributes when both are present. Pass <c>null</c> to use default resolution.
        /// </param>
        public static void ExecuteMerge<T>(this SqlConnection connection, IEnumerable<T> data, string tableName, string key, IDictionary<string, string>? columnMappings) where T : class
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentException("Key cannot be null or empty.", nameof(key));
            ExecuteMerge(connection, data, tableName, new[] { key }, columnMappings);
        }

        /// <summary>
        /// Executes a MERGE statement with a composite key.
        /// </summary>
        /// <typeparam name="T">The model type whose properties map to table columns.</typeparam>
        /// <param name="connection">An open <see cref="SqlConnection"/>.</param>
        /// <param name="data">The source data to merge.</param>
        /// <param name="tableName">The target table name.</param>
        /// <param name="keys">The C# property names that form the composite merge key.</param>
        public static void ExecuteMerge<T>(this SqlConnection connection, IEnumerable<T> data, string tableName, string[] keys) where T : class
        {
            ExecuteMerge(connection, data, tableName, keys, (IDictionary<string, string>?)null);
        }

        /// <summary>
        /// Executes a MERGE statement with a composite key, using EF Core metadata from the provided DbContext instance.
        /// </summary>
        /// <typeparam name="T">The model type whose properties map to table columns.</typeparam>
        /// <param name="connection">An open <see cref="SqlConnection"/>.</param>
        /// <param name="data">The source data to merge.</param>
        /// <param name="tableName">The target table name.</param>
        /// <param name="keys">The C# property names that form the composite merge key.</param>
        /// <param name="dbContext">An EF Core <c>DbContext</c> instance used to resolve Fluent API mappings.</param>
        public static void ExecuteMerge<T>(this SqlConnection connection, IEnumerable<T> data, string tableName, string[] keys, object dbContext) where T : class
        {
            EfCoreMappingResolver.ValidateDbContext(dbContext);
            var mappings = EfCoreMappingResolver.TryGetColumnMappings(dbContext, typeof(T));
            ExecuteMerge(connection, data, tableName, keys, mappings);
        }

        /// <summary>
        /// Executes a MERGE statement with a composite key and explicit column mappings.
        /// </summary>
        /// <typeparam name="T">The model type whose properties map to table columns.</typeparam>
        /// <param name="connection">An open <see cref="SqlConnection"/>.</param>
        /// <param name="data">The source data to merge.</param>
        /// <param name="tableName">The target table name.</param>
        /// <param name="keys">The C# property names that form the composite merge key.</param>
        /// <param name="columnMappings">
        /// A dictionary mapping C# property names to SQL column names.
        /// Overrides [Column] attributes when both are present. Pass <c>null</c> to use default resolution.
        /// </param>
        public static void ExecuteMerge<T>(this SqlConnection connection, IEnumerable<T> data, string tableName, string[] keys, IDictionary<string, string>? columnMappings) where T : class
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (data == null) throw new ArgumentNullException(nameof(data));
            if (string.IsNullOrEmpty(tableName)) throw new ArgumentException("Table name cannot be null or empty.", nameof(tableName));
            if (keys == null || keys.Length == 0) throw new ArgumentException("At least one key is required.", nameof(keys));

            var dataTable = ToDataTable(data, columnMappings);

            var createTempTableQuery = $"SELECT TOP 0 * INTO #SourceTable FROM [{tableName}]";
            using (var cmd = new SqlCommand(createTempTableQuery, connection)) cmd.ExecuteNonQuery();

            try
            {
                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = "#SourceTable";
                    bulkCopy.WriteToServer(dataTable);
                }

                var builder = new MergeBuilder<T>()
                    .IntoTable(tableName)
                    .WithCompositeKey(keys);

                if (columnMappings != null)
                {
                    foreach (var kvp in columnMappings)
                        builder.WithColumnMapping(kvp.Key, kvp.Value);
                }

                using (var cmd = new SqlCommand(builder.Build(), connection)) cmd.ExecuteNonQuery();
            }
            finally
            {
                using (var cmd = new SqlCommand("DROP TABLE IF EXISTS #SourceTable", connection)) cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Asynchronously executes a MERGE statement against the specified table using the provided data.
        /// Column names are resolved from [Column] attributes or property names.
        /// </summary>
        /// <typeparam name="T">The model type whose properties map to table columns.</typeparam>
        /// <param name="connection">An open <see cref="SqlConnection"/>.</param>
        /// <param name="data">The source data to merge.</param>
        /// <param name="tableName">The target table name.</param>
        /// <param name="key">The C# property name used as the merge key.</param>
        /// <param name="cancellationToken">A token to cancel the operation.</param>
        public static Task ExecuteMergeAsync<T>(this SqlConnection connection, IEnumerable<T> data, string tableName, string key, CancellationToken cancellationToken = default) where T : class
        {
            return ExecuteMergeAsync(connection, data, tableName, key, (IDictionary<string, string>?)null, cancellationToken);
        }

        /// <summary>
        /// Asynchronously executes a MERGE statement using EF Core metadata from the provided DbContext instance.
        /// </summary>
        /// <typeparam name="T">The model type whose properties map to table columns.</typeparam>
        /// <param name="connection">An open <see cref="SqlConnection"/>.</param>
        /// <param name="data">The source data to merge.</param>
        /// <param name="tableName">The target table name.</param>
        /// <param name="key">The C# property name used as the merge key.</param>
        /// <param name="dbContext">An EF Core <c>DbContext</c> instance used to resolve Fluent API mappings.</param>
        /// <param name="cancellationToken">A token to cancel the operation.</param>
        public static Task ExecuteMergeAsync<T>(this SqlConnection connection, IEnumerable<T> data, string tableName, string key, object dbContext, CancellationToken cancellationToken = default) where T : class
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentException("Key cannot be null or empty.", nameof(key));
            EfCoreMappingResolver.ValidateDbContext(dbContext);
            var mappings = EfCoreMappingResolver.TryGetColumnMappings(dbContext, typeof(T));
            return ExecuteMergeAsync(connection, data, tableName, new[] { key }, mappings, cancellationToken);
        }

        /// <summary>
        /// Asynchronously executes a MERGE statement against the specified table using the provided data,
        /// with explicit column mappings for properties whose SQL column names differ from C# property names
        /// (e.g. when configured via EF Core's <c>OnModelCreating</c>).
        /// </summary>
        /// <typeparam name="T">The model type whose properties map to table columns.</typeparam>
        /// <param name="connection">An open <see cref="SqlConnection"/>.</param>
        /// <param name="data">The source data to merge.</param>
        /// <param name="tableName">The target table name.</param>
        /// <param name="key">The C# property name used as the merge key.</param>
        /// <param name="columnMappings">
        /// A dictionary mapping C# property names to SQL column names.
        /// Overrides [Column] attributes when both are present. Pass <c>null</c> to use default resolution.
        /// </param>
        /// <param name="cancellationToken">A token to cancel the operation.</param>
        public static Task ExecuteMergeAsync<T>(this SqlConnection connection, IEnumerable<T> data, string tableName, string key, IDictionary<string, string>? columnMappings, CancellationToken cancellationToken = default) where T : class
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentException("Key cannot be null or empty.", nameof(key));
            return ExecuteMergeAsync(connection, data, tableName, new[] { key }, columnMappings, cancellationToken);
        }

        /// <summary>
        /// Asynchronously executes a MERGE statement with a composite key.
        /// </summary>
        /// <typeparam name="T">The model type whose properties map to table columns.</typeparam>
        /// <param name="connection">An open <see cref="SqlConnection"/>.</param>
        /// <param name="data">The source data to merge.</param>
        /// <param name="tableName">The target table name.</param>
        /// <param name="keys">The C# property names that form the composite merge key.</param>
        /// <param name="cancellationToken">A token to cancel the operation.</param>
        public static Task ExecuteMergeAsync<T>(this SqlConnection connection, IEnumerable<T> data, string tableName, string[] keys, CancellationToken cancellationToken = default) where T : class
        {
            return ExecuteMergeAsync(connection, data, tableName, keys, (IDictionary<string, string>?)null, cancellationToken);
        }

        /// <summary>
        /// Asynchronously executes a MERGE statement with a composite key, using EF Core metadata.
        /// </summary>
        /// <typeparam name="T">The model type whose properties map to table columns.</typeparam>
        /// <param name="connection">An open <see cref="SqlConnection"/>.</param>
        /// <param name="data">The source data to merge.</param>
        /// <param name="tableName">The target table name.</param>
        /// <param name="keys">The C# property names that form the composite merge key.</param>
        /// <param name="dbContext">An EF Core <c>DbContext</c> instance used to resolve Fluent API mappings.</param>
        /// <param name="cancellationToken">A token to cancel the operation.</param>
        public static Task ExecuteMergeAsync<T>(this SqlConnection connection, IEnumerable<T> data, string tableName, string[] keys, object dbContext, CancellationToken cancellationToken = default) where T : class
        {
            EfCoreMappingResolver.ValidateDbContext(dbContext);
            var mappings = EfCoreMappingResolver.TryGetColumnMappings(dbContext, typeof(T));
            return ExecuteMergeAsync(connection, data, tableName, keys, mappings, cancellationToken);
        }

        /// <summary>
        /// Asynchronously executes a MERGE statement with a composite key and explicit column mappings.
        /// </summary>
        /// <typeparam name="T">The model type whose properties map to table columns.</typeparam>
        /// <param name="connection">An open <see cref="SqlConnection"/>.</param>
        /// <param name="data">The source data to merge.</param>
        /// <param name="tableName">The target table name.</param>
        /// <param name="keys">The C# property names that form the composite merge key.</param>
        /// <param name="columnMappings">
        /// A dictionary mapping C# property names to SQL column names.
        /// Overrides [Column] attributes when both are present. Pass <c>null</c> to use default resolution.
        /// </param>
        /// <param name="cancellationToken">A token to cancel the operation.</param>
        public static async Task ExecuteMergeAsync<T>(this SqlConnection connection, IEnumerable<T> data, string tableName, string[] keys, IDictionary<string, string>? columnMappings, CancellationToken cancellationToken = default) where T : class
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (data == null) throw new ArgumentNullException(nameof(data));
            if (string.IsNullOrEmpty(tableName)) throw new ArgumentException("Table name cannot be null or empty.", nameof(tableName));
            if (keys == null || keys.Length == 0) throw new ArgumentException("At least one key is required.", nameof(keys));

            var dataTable = ToDataTable(data, columnMappings);

            var createTempTableQuery = $"SELECT TOP 0 * INTO #SourceTable FROM [{tableName}]";
            using (var cmd = new SqlCommand(createTempTableQuery, connection)) await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = "#SourceTable";
                    await bulkCopy.WriteToServerAsync(dataTable, cancellationToken).ConfigureAwait(false);
                }

                var builder = new MergeBuilder<T>()
                    .IntoTable(tableName)
                    .WithCompositeKey(keys);

                if (columnMappings != null)
                {
                    foreach (var kvp in columnMappings)
                        builder.WithColumnMapping(kvp.Key, kvp.Value);
                }

                using (var cmd = new SqlCommand(builder.Build(), connection)) await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                using (var cmd = new SqlCommand("DROP TABLE IF EXISTS #SourceTable", connection)) await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        internal static DataTable ToDataTable<T>(IEnumerable<T> data, IDictionary<string, string>? columnMappings = null)
        {
            var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            var table = new DataTable();
            foreach (var prop in properties)
            {
                var columnName = ResolveColumnName(prop, columnMappings);
                table.Columns.Add(columnName, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
            }
            foreach (var item in data)
            {
                var row = table.NewRow();
                foreach (var prop in properties)
                {
                    var columnName = ResolveColumnName(prop, columnMappings);
                    row[columnName] = prop.GetValue(item) ?? DBNull.Value;
                }
                table.Rows.Add(row);
            }
            return table;
        }

        private static string ResolveColumnName(PropertyInfo property, IDictionary<string, string>? columnMappings)
        {
            if (columnMappings != null && columnMappings.TryGetValue(property.Name, out var mapped))
                return mapped;

            return ColumnNameResolver.GetColumnName(property);
        }
    }
}
