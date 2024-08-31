namespace Arindu.Features.Historify.Entities;

public class TableConfiguration
{
    public string SourceConnectionString { get; set; }
    public string DestinationConnectionString { get; set; }
    public string TableName { get; set; }
    public string SchemaName { get; set; }
    public string DateColumnName { get; set; }
    public int DaysInterval { get; set; }
    public string DestinationSchemaName { get; set; }
    public int BatchSize { get; set; }
}
