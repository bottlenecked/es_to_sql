using System.Reflection;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

public class Database
{
  public static SqlConnection CreateSqlConnection(IConfigurationRoot config)
  {
    var connection = new SqlConnection(config["DATABASE_CONNECTION_STRING"]);
    connection.Open();
    return connection;
  }

  public static void CreateLogTableIfNotExists(SqlConnection conn)
  {
    var table_name = "log_entries";

    ExecuteSql(conn, @$"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' and xtype='U')
                      CREATE TABLE {table_name} (
                          index_name varchar(255) not null,
                          inserted_at datetime not null
                      )");
  }

  public static IEnumerable<string> DetermineIndexesToFetchDataFrom(IEnumerable<string> allIndexes, SqlConnection conn)
  {
    var last_index = ExecuteSql(conn, "select top(1) * from log_entries order by inserted_at desc").Select(r => (string)r["index_name"]).FirstOrDefault();
    return allIndexes.Order().SkipWhile(index => string.Compare(index, last_index) < 1);
  }

  public static IEnumerable<Dictionary<string, object>> ExecuteSql(SqlConnection conn, string cmdText, object? parameters = null)
  {
    using (var cmd = new SqlCommand(cmdText, conn))
    {
      if (parameters != null)
      {
        foreach (var prop in parameters.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
          cmd.Parameters.AddWithValue($"@{prop.Name}", prop.GetValue(parameters, null));
        }
      }
      using (var reader = cmd.ExecuteReader())
      {
        var list = new List<Dictionary<string, object>>();
        while (reader.Read())
        {
          var values = new object[reader.FieldCount];
          reader.GetValues(values);
          var row = new Dictionary<string, object>();
          foreach (var col in reader.GetColumnSchema())
          {
            var value = values[col.ColumnOrdinal.GetValueOrDefault(0)];
            row.Add(col.ColumnName, value);
            list.Add(row);
          }
        }
        return list;
      }
    }
  }
}