using System.Reflection;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json.Linq;

internal class Program
{
  static IConfigurationRoot config = CreateConfig();


  private static async Task Main(string[] args)
  {
    var indexes = await ListIndexes();
    Console.WriteLine(string.Join(", ", indexes));
    var connection = CreateSqlConnection();
    CreateLogTableIfNotExists(connection);
    var indexesToScrape = DetermineIndexesToFetchDataFrom(indexes, connection);
    Console.WriteLine(string.Join(", ", indexesToScrape));
  }

  public static IConfigurationRoot CreateConfig()
  {
    return new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json", true)
        .AddEnvironmentVariables()
        .Build();
  }

  public static async Task<IEnumerable<string>> ListIndexes()
  {
    using (var client = CreateElasticSearchClient())
    {
      var json = await client.GetStringAsync("/_cat/indices");
      return JArray.Parse(json)
              .Select(x => (string)x["index"])
              .ToList();
    }
  }

  public static HttpClient CreateElasticSearchClient()
  {
    var authStr = Convert.ToBase64String(System.Text.ASCIIEncoding.UTF8.GetBytes($"{config["ES_USER_NAME"]}:{config["ES_PASSWORD"]}"));
    return new HttpClient
    {
      BaseAddress = new Uri(config["ES_BASE_URL"]),
      DefaultRequestHeaders = {
        { "Authorization", "Basic " + authStr },
        {"Accept", "application/json"}
      }
    };
  }

  public static SqlConnection CreateSqlConnection()
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

  public static IEnumerable<string> DetermineIndexesToFetchDataFrom(IEnumerable<string> allIndexes, SqlConnection conn)
  {
    var last_index = ExecuteSql(conn, "select top(1) * from log_entries order by inserted_at desc").Select(r => (string)r["index_name"]).FirstOrDefault();
    return allIndexes.Order().SkipWhile(index => string.Compare(index, last_index) < 1);
  }

}
