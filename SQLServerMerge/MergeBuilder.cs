using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace SQLServerMerge
{
    /// <summary>
    /// Fluent builder for constructing a SQL Server MERGE statement from a typed model.
    /// </summary>
    /// <typeparam name="T">The model type whose properties map to table columns.</typeparam>
    public class MergeBuilder<T> where T : class
    {
        private string? _tableName;
        private readonly List<string> _keyProperties = new List<string>();
        private readonly List<string> _columnsToIgnore = new List<string>();
        private readonly Dictionary<string, string> _columnMappings = new Dictionary<string, string>();
        private bool _deleteOrphans;

        /// <summary>
        /// Sets the target table name for the MERGE statement.
        /// </summary>
        public MergeBuilder<T> IntoTable(string tableName)
        {
            _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            return this;
        }

        /// <summary>
        /// Sets the key property used for matching source and target rows.
        /// </summary>
        /// <param name="keyProperty">The C# property name used as the merge key.</param>
        public MergeBuilder<T> WithKey(string keyProperty)
        {
            if (keyProperty == null) throw new ArgumentNullException(nameof(keyProperty));
            if (keyProperty.Length == 0) throw new ArgumentException("Key property name cannot be empty.", nameof(keyProperty));
            _keyProperties.Clear();
            _keyProperties.Add(keyProperty);
            return this;
        }

        /// <summary>
        /// Sets multiple key properties for a composite key used for matching source and target rows.
        /// </summary>
        /// <param name="keyProperties">The C# property names that form the composite merge key.</param>
        public MergeBuilder<T> WithCompositeKey(params string[] keyProperties)
        {
            if (keyProperties == null) throw new ArgumentNullException(nameof(keyProperties));
            if (keyProperties.Length == 0) throw new ArgumentException("At least one key property is required.", nameof(keyProperties));
            _keyProperties.Clear();
            foreach (var key in keyProperties)
            {
                if (string.IsNullOrEmpty(key)) throw new ArgumentException("Key property names cannot be null or empty.", nameof(keyProperties));
                _keyProperties.Add(key);
            }
            return this;
        }

        /// <summary>
        /// Excludes a property from the generated MERGE statement.
        /// </summary>
        /// <param name="propertyName">The C# property name to exclude.</param>
        public MergeBuilder<T> Ignore(string propertyName)
        {
            _columnsToIgnore.Add(propertyName ?? throw new ArgumentNullException(nameof(propertyName)));
            return this;
        }

        /// <summary>
        /// Maps a C# property name to a specific SQL column name, overriding any [Column] attribute.
        /// </summary>
        /// <param name="propertyName">The C# property name.</param>
        /// <param name="columnName">The SQL column name.</param>
        public MergeBuilder<T> WithColumnMapping(string propertyName, string columnName)
        {
            if (propertyName == null) throw new ArgumentNullException(nameof(propertyName));
            if (columnName == null) throw new ArgumentNullException(nameof(columnName));
            _columnMappings[propertyName] = columnName;
            return this;
        }

        /// <summary>
        /// Enables deletion of target rows that have no matching source row.
        /// Adds a <c>WHEN NOT MATCHED BY SOURCE THEN DELETE</c> clause to the generated MERGE statement.
        /// </summary>
        public MergeBuilder<T> WithDeleteOrphans()
        {
            _deleteOrphans = true;
            return this;
        }

        /// <summary>
        /// Builds the MERGE SQL statement.
        /// </summary>
        /// <returns>The generated MERGE SQL string.</returns>
        /// <exception cref="InvalidOperationException">Thrown when table name or key property is not set.</exception>
        public string Build()
        {
            if (string.IsNullOrEmpty(_tableName)) throw new InvalidOperationException("Table name is required. Call IntoTable() first.");
            if (_keyProperties.Count == 0) throw new InvalidOperationException("Key property is required. Call WithKey() or WithCompositeKey() first.");

            var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => !_columnsToIgnore.Contains(p.Name))
                .ToList();

            var keyColumnNames = _keyProperties.Select(k =>
                ResolveColumnName(properties.FirstOrDefault(p => p.Name == k), k)).ToList();
            var updateProperties = properties.Where(p => !_keyProperties.Contains(p.Name)).ToList();

            var sb = new StringBuilder();
            sb.AppendLine($"MERGE INTO [{_tableName}] AS Target");
            sb.AppendLine("USING #SourceTable AS Source");
            sb.AppendLine($"ON {string.Join(" AND ", keyColumnNames.Select(k => $"Target.[{k}] = Source.[{k}]"))}");

            if (updateProperties.Count > 0)
            {
                sb.AppendLine("WHEN MATCHED THEN");
                sb.Append("    UPDATE SET ");
                sb.AppendLine(string.Join(", ", updateProperties.Select(p =>
                {
                    var col = ResolveColumnName(p);
                    return $"Target.[{col}] = Source.[{col}]";
                })));
            }

            sb.AppendLine("WHEN NOT MATCHED BY TARGET THEN");
            sb.Append("    INSERT (");
            sb.Append(string.Join(", ", properties.Select(p => $"[{ResolveColumnName(p)}]")));
            sb.AppendLine(")");
            sb.Append("    VALUES (");
            sb.Append(string.Join(", ", properties.Select(p => $"Source.[{ResolveColumnName(p)}]")));
            sb.AppendLine(")");

            if (_deleteOrphans)
            {
                sb.AppendLine("WHEN NOT MATCHED BY SOURCE THEN");
                sb.Append("    DELETE");
            }

            sb.Append(";");

            return sb.ToString();
        }

        private string ResolveColumnName(PropertyInfo? property, string? fallback = null)
        {
            if (property != null && _columnMappings.TryGetValue(property.Name, out var mapped))
                return mapped;

            if (property != null)
                return ColumnNameResolver.GetColumnName(property);

            return fallback ?? throw new InvalidOperationException("Unable to resolve column name.");
        }
    }
}
