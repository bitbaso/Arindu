using MySql.Data.MySqlClient;
using Dapper;
using Arindu.Features.TestData.Interfaces;

namespace Arindu.Features.TestData;

public class TestDataManager(ILogger<TestDataManager> logger) : ITestDataManager
{
    #region Public Methods
    public async Task InsertRandomRowsAsync(int numberOfRows, string connectionString)
    {
        try
        {
            var random = new Random();

            var rows = new List<dynamic>();
            for (int i = 0; i < numberOfRows; i++)
            {
                var daysBefore = random.Next(1, 5000);

                rows.Add(new
                {
                    created = DateTime.Now.AddDays(-daysBefore),
                    text = GenerateRandomText(10)
                });
            }

            var insertQuery = "INSERT INTO `full` (`created`, `text`) VALUES (@created, @text)";

            using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        await connection.ExecuteAsync(insertQuery, rows, transaction);
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        logger.LogError($"Error inserting rows: {ex.Message}");
                        throw;
                    }
                }
            }

            logger.LogInformation("TEST DATA INSERT OK");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "ERROR InsertRandomRowsAsync");
        }

    }
    #endregion Public Methods

    #region Private Methods
    private string GenerateRandomText(int length)
    {
        var random = new Random();
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        var text = new char[length];
        for (int i = 0; i < length; i++)
        {
            text[i] = chars[random.Next(chars.Length)];
        }
        return new string(text);
    }
    #endregion Private Methods
}
