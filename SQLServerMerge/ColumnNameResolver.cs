using System.Linq;
using System.Reflection;

namespace SQLServerMerge
{
    /// <summary>
    /// Resolves the SQL column name for a property by checking for a [Column] attribute.
    /// Uses reflection to avoid a hard dependency on System.ComponentModel.Annotations.
    /// </summary>
    internal static class ColumnNameResolver
    {
        internal static string GetColumnName(PropertyInfo property)
        {
            var columnAttr = property.GetCustomAttributes(true)
                .FirstOrDefault(a => a.GetType().FullName == "System.ComponentModel.DataAnnotations.Schema.ColumnAttribute");

            if (columnAttr != null)
            {
                var nameProp = columnAttr.GetType().GetProperty("Name");
                if (nameProp != null)
                {
                    var name = nameProp.GetValue(columnAttr) as string;
                    if (!string.IsNullOrEmpty(name))
                        return name;
                }
            }

            return property.Name;
        }
    }
}
