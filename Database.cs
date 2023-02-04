using System.Collections;
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

  public static Task CreateLogTableIfNotExists(SqlConnection conn)
  {
    var table_name = "log_entries";

    return ExecuteSql(conn, @$"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' and xtype='U')
                      BEGIN
                        CREATE TABLE {table_name} (
                            id bigint NOT NULL IDENTITY(1,1) PRIMARY KEY,
                            index_name varchar(255) not null,
                            inserted_at datetime not null
                        );
                        CREATE UNIQUE INDEX uidx_index_name ON {table_name} (index_name);
                      END");
  }
  public static Task CreateDocumentsTableIfNotExists(SqlConnection conn)
  {
    var table_name = "documents";

    return ExecuteSql(conn, @$"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' and xtype='U')
                        BEGIN
                          CREATE TABLE {table_name} (
                              id bigint NOT NULL IDENTITY(1,1) PRIMARY KEY,
                              document_id varchar(255) NOT NULL,
                              index_name varchar(255) not null,
                              event_category varchar(255),
                              event_timestamp datetime not null,
                              event_type varchar(255),
                              host varchar(255),
                              syslog_hostname varchar(255),
                              source_zone varchar(255),
                              application varchar(255),
                              reason varchar(512),
                              category varchar(255),
                              url varchar(512),
                              attack_name varchar(255),
                              threat_severity varchar(32),
                              inserted_at datetime not null
                          );
                          CREATE UNIQUE INDEX uidx_document_id_index_name ON {table_name} (document_id, index_name);
                      END");
  }

  public static async Task<IEnumerable<Dictionary<string, object>>> ExecuteSql(SqlConnection conn, string cmdText, object? parameters = null)
  {
    using (var cmd = new SqlCommand(cmdText, conn))
    {
      foreach (var prop in UnrollParams(parameters))
      {
        // this should allow us to write parameterized WHERE x in (...) queries
        if (prop.Value.GetType() != typeof(string) && prop.Value is IEnumerable list)
        {
          int i = 0;
          foreach (var val in list)
          {
            cmd.Parameters.AddWithValue($"@{prop.Key}{i++}", val);
          }
          string replacementParamText = string.Join(",", Enumerable.Range(0, i).Select(i => $"@{prop.Key}{i}"));
          cmd.CommandText = cmd.CommandText.Replace($"@{prop.Key}", replacementParamText);
        }
        else
        {
          cmd.Parameters.AddWithValue($"@{prop.Key}", prop.Value);
        }
      }
      using (var reader = await cmd.ExecuteReaderAsync())
      {
        var list = new List<Dictionary<string, object>>();
        while (await reader.ReadAsync())
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

  private static IEnumerable<KeyValuePair<string, object>> UnrollParams(object? cmdparams)
  {
    if (cmdparams == null)
    {
      yield break;
    }
    else if (cmdparams is IDictionary<string, object> dict)
    {
      foreach (var kvp in dict)
      {
        yield return KeyValuePair.Create<string, object>(kvp.Key, kvp.Value ?? DBNull.Value);
      }
    }
    else
    {
      foreach (var prop in cmdparams.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public))
      {
        yield return KeyValuePair.Create<string, object>(prop.Name, prop.GetValue(cmdparams, null) ?? DBNull.Value);
      }
    }
  }
}
