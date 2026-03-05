using System;
using System.Collections;
using System.Collections.Generic;

namespace SQLServerMerge
{
    /// <summary>
    /// Resolves EF Core Fluent API column mappings via reflection without taking a dependency on EF Core.
    /// </summary>
    internal static class EfCoreMappingResolver
    {
        /// <summary>
        /// Validates that the provided object is an EF Core DbContext (by checking for a <c>Model</c> property).
        /// </summary>
        internal static void ValidateDbContext(object dbContext)
        {
            if (dbContext == null) throw new ArgumentNullException(nameof(dbContext));

            var modelProperty = dbContext.GetType().GetProperty("Model");
            if (modelProperty == null)
            {
                throw new ArgumentException(
                    $"The object of type '{dbContext.GetType().FullName}' does not appear to be an EF Core DbContext. " +
                    "Expected a 'Model' property. Pass a DbContext instance or use the IDictionary<string, string> overload instead.",
                    nameof(dbContext));
            }
        }

        internal static IDictionary<string, string>? TryGetColumnMappings(object dbContext, Type entityClrType)
        {
            var modelProperty = dbContext.GetType().GetProperty("Model");
            var model = modelProperty!.GetValue(dbContext);
            if (model == null) return null;

            var findEntityType = model.GetType().GetMethod("FindEntityType", new[] { typeof(string) });
            if (findEntityType == null) return null;

            var entityType = findEntityType.Invoke(model, new object[] { entityClrType.FullName! });
            if (entityType == null) return null;

            var getProperties = FindInterfaceMethod(entityType.GetType(), "GetProperties");
            if (getProperties == null) return null;

            var properties = getProperties.Invoke(entityType, null) as IEnumerable;
            if (properties == null) return null;

            var mappings = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var prop in properties)
            {
                if (prop == null) continue;

                var nameProp = prop.GetType().GetProperty("Name");
                var clrName = nameProp?.GetValue(prop) as string;
                if (string.IsNullOrEmpty(clrName)) continue;

                var columnName = GetRelationalColumnName(prop) ?? clrName;
                mappings[clrName] = columnName;
            }

            return mappings.Count > 0 ? mappings : null;
        }

        private static string? GetRelationalColumnName(object property)
        {
            var findAnnotation = property.GetType().GetMethod("FindAnnotation", new[] { typeof(string) })
                ?? FindInterfaceMethod(property.GetType(), "FindAnnotation", new[] { typeof(string) });
            if (findAnnotation == null) return null;

            var annotation = findAnnotation.Invoke(property, new object[] { "Relational:ColumnName" });
            if (annotation == null) return null;

            var valueProperty = annotation.GetType().GetProperty("Value");
            return valueProperty?.GetValue(annotation) as string;
        }

        private static System.Reflection.MethodInfo? FindInterfaceMethod(Type type, string methodName, Type[]? parameterTypes = null)
        {
            parameterTypes = parameterTypes ?? Type.EmptyTypes;
            foreach (var iface in type.GetInterfaces())
            {
                var method = iface.GetMethod(methodName, parameterTypes);
                if (method != null) return method;
            }
            return null;
        }
    }
}
