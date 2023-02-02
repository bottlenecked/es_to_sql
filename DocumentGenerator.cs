using System.Collections.Concurrent;
using System.Net;
using System.Text;

class DocumentGenerator
{
  public static async Task<IEnumerable<string>> CreateIndexes(HttpClient elasticClient)
  {
    var existingIndexes = (await Elastic.ListIndexesOrdered(elasticClient)).ToList();
    var q = new ConcurrentQueue<string>();
    await Parallel.ForEachAsync(new[] { 1, 2 }, async (day, _ct) =>
    {
      await Parallel.ForEachAsync(Enumerable.Range(0, 24), async (time, _ct) =>
      {
        var indexName = $"junoslogs-2023.01.{day.ToString("D2")}-{time.ToString("D2")}";
        if (!existingIndexes.Contains(indexName))
        {
          Console.WriteLine($"Creating index {indexName}...");
          await elasticClient.PutAsync($"/{indexName}", null);
        }
        q.Enqueue(indexName);
      });
    });
    return q.ToList();
  }

  public static async Task PopulateIndexes(HttpClient elasticClient, IEnumerable<string> indexes)
  {
    var document = @"
    {
      ""src_nat_rule_name"": ""N/A"",
      ""packets_from_server"": ""1"",
      ""reason"": ""Closed by junos-dynapp"",
      ""nat_source_port"": ""6771"",
      ""domain_id"": 425985,
      ""profile_name"": ""N/A"",
      ""event_type"": ""APPTRACK_SESSION_CLOSE"",
      ""routing_instance"": ""N/A"",
      ""host"": ""10.255.12.235"",
      ""source_zone_name"": ""business-Wired"",
      ""packets_from_client"": ""2"",
      ""receiving_host"": ""10.253.0.100"",
      ""device_id"": 1769476,
      ""nat_destination_address"": ""239.192.152.143"",
      ""rule_name"": ""N/A"",
      ""service_name"": ""None"",
      ""uplink_tx_bytes"": ""0"",
      ""uplink_rx_bytes"": ""0"",
      ""protocol_id"": ""17"",
      ""priority"": ""14"",
      ""dst_nat_rule_name"": ""N/A"",
      ""destination_zone_name"": ""business-Wired"",
      ""nat_source_address"": ""172.16.27.116"",
      ""bytes_from_client"": ""366"",
      ""destination_port"": ""6771"",
      ""sub_category"": ""File-Sharing"",
      ""roles"": ""N/A"",
      ""uplink_incoming_interface_name"": ""N/A"",
      ""destination_interface_name"": ""ge-0/0/3.0"",
      ""event_timestamp"": ""2023-02-01T08:03:28+00:00"",
      ""source_address"": ""172.16.27.116"",
      ""source_port"": ""6771"",
      ""multipath_rule_name"": ""N/A"",
      ""elapsed_time"": ""4"",
      ""bytes_from_server"": ""74"",
      ""severity"": 0,
      ""destination_address"": ""239.192.152.143"",
      ""nat_destination_port"": ""6771"",
      ""policy_name"": ""business-wired-appl-blocks"",
      ""message"": ""<14>1 2023-02-01T08:03:28+00:00 vSRX.apollogr RT_FLOW - APPTRACK_SESSION_CLOSE [junos@2636.1.1.1.2.129 reason=\""Closed by junos-dynapp\"" source-address=\""172.16.27.116\"" source-port=\""6771\"" destination-address=\""239.192.152.143\"" destination-port=\""6771\"" service-name=\""None\"" application=\""BITTORRENT\"" nested-application=\""UNKNOWN\"" nat-source-address=\""172.16.27.116\"" nat-source-port=\""6771\"" nat-destination-address=\""239.192.152.143\"" nat-destination-port=\""6771\"" src-nat-rule-name=\""N/A\"" dst-nat-rule-name=\""N/A\"" protocol-id=\""17\"" policy-name=\""business-wired-appl-blocks\"" source-zone-name=\""business-Wired\"" destination-zone-name=\""business-Wired\"" session-id-32=\""111929\"" packets-from-client=\""2\"" bytes-from-client=\""366\"" packets-from-server=\""1\"" bytes-from-server=\""74\"" elapsed-time=\""4\"" username=\""N/A\"" roles=\""N/A\"" encrypted=\""No\"" profile-name=\""N/A\"" rule-name=\""N/A\"" routing-instance=\""N/A\"" destination-interface-name=\""ge-0/0/3.0\"" uplink-incoming-interface-name=\""N/A\"" uplink-tx-bytes=\""0\"" uplink-rx-bytes=\""0\"" category=\""P2P\"" sub-category=\""File-Sharing\"" apbr-policy-name=\""N/A\"" multipath-rule-name=\""N/A\""]\n"",
      ""event_category"": ""apptrack"",
      ""nested_application"": ""UNKNOWN"",
      ""@timestamp"": ""2023-02-01T08:22:54.040Z"",
      ""syslog_hostname"": ""vSRX.apollogr"",
      ""application"": ""BITTORRENT"",
      ""encrypted"": ""No"",
      ""apbr_policy_name"": ""N/A"",
      ""session_id_32"": ""111929"",
      ""category"": ""P2P"",
      ""facility"": 0,
      ""username"": ""N/A""
    }";

    await Parallel.ForEachAsync(indexes, async (index, _ct) =>
    {
      await Parallel.ForEachAsync(Enumerable.Range(1, 100), async (i, _ct) =>
      {
        if (i % 50 == 0)
        {
          Console.WriteLine($"Creating document {i} on index {index}");
        }
        var content = await elasticClient.PutAsync($"/{index}/syslogs/{i}", new StringContent(document, Encoding.UTF8, "application/json"));
        if ((int)content.StatusCode > 201)
        {
          throw new Exception(@$"
          Status code: {content.StatusCode}
          Headers: {String.Join("; ", content.Headers.Select(h => $"{h.Key}={h.Value}"))}
          Body: {await content.Content.ReadAsStringAsync()}
          ");
        }
      });
    });
  }
}
