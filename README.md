# Arindu

Arindu is an experimental worker service built in C# that facilitates the archiving of old records from specified tables in a source database to a destination database. It helps manage database size by transferring records older than a defined interval to a history database and deleting them from the source database. This configuration-driven service operates based on settings defined in the `appsettings.json` file.

The name **"Arindu"** comes from Euskera (Basque language) and means **"to lighten" or "to relieve,"** and is related to the word **"arin,"** which means **"fast."** This reflects the service's goal of quickly reducing the load on the source database by efficiently archiving outdated data, making the database lighter and more manageable.

## Features

- **Configurable Table Archiving**: Easily configure which tables to archive by specifying connection strings, table names, schema names, and more in the configuration file.
- **Batch Processing**: Move data in configurable batch sizes to avoid performance issues.
- **Automated Deletion**: Automatically deletes old records from the source table after archiving them to the destination table.
- **Scheduled Execution**: Runs at regular intervals as defined in the configuration.

## Configuration

The service behavior is driven by the `appsettings.json` file. Below is an example configuration:

```json
{
  "TablesConfiguration": [
    {
      "SourceConnectionString": "server=10.175.147.39;user=root;database=test;port=32768;password=test;SslMode=none;",
      "DestinationConnectionString": "server=10.175.147.39;user=root;database=test_history;port=32768;password=test;SslMode=none;",
      "TableName": "full",
      "SchemaName": "test",
      "DateColumnName": "created",
      "BatchSize": 400,
      "DaysInterval": 500,
      "DestinationSchemaName": "test_history"
    }
  ],
  "LoopTimeMilliseconds": 180000
}
```

### Configuration Fields
- **SourceConnectionString**: Connection string for the source database where the original records are stored.
- **DestinationConnectionString**: Connection string for the destination database where old records will be archived.
- **TableName**: Name of the table to archive.
- **SchemaName**: Schema of the source table.
- **DateColumnName**: Column name used to determine the age of records (typically a date or timestamp).
- **BatchSize**: Number of records to process in each batch.
- **DaysInterval**: Number of days to retain records in the source table. Records older than this interval will be moved to the destination.
- **DestinationSchemaName**: Schema of the destination table.
- **LoopTimeMilliseconds**: Time in milliseconds between each execution of the archiving process.

## Prerequisites
- .NET SDK 8.0 or later
- Access to the source and destination databases
- Proper permissions to read from the source database and write to the destination database

## Getting Started
### Clone the Repository
```bash
git clone https://github.com/yourusername/arindu.git
cd arindu
```

### Build the Project
Ensure you have .NET SDK installed, then build the project using:

```bash
dotnet build
```

## Running the Service
1- Edit the appsettings.json file to configure the connection strings, tables, and other parameters.

2- Run the service using:

```bash
dotnet run
```
The service will start and begin processing tables based on the configurations provided.

## Testing
Arindu has been tested with MySQL databases. While it may work with other databases that support similar configurations, compatibility is not guaranteed.

## Disclaimer
Arindu is an experimental project. While it has been tested with MySQL, use it at your own risk. The authors are not responsible for any data loss, corruption, or other issues that may arise from using this software. Always test thoroughly in a safe environment before deploying to production.

## Deployment
To deploy the worker service, publish the application using the following command:

```bash
dotnet publish -c Release -o ./publish
```
You can then deploy the published files to your server or cloud service, and run the service executable.

## Logging and Monitoring
Arindu uses built-in .NET logging mechanisms. You can configure logging providers such as console, file, or other third-party services via the appsettings.json file.

## Contributing
Contributions are welcome! Please fork the repository and submit a pull request for any bug fixes or enhancements.

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Contact
For any questions or feedback, please open an issue on the GitHub repository.