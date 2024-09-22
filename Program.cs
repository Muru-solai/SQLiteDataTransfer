
using Microsoft.Extensions.Configuration;
using Microsoft.Data;
using Microsoft.Data.Sqlite;
using System.Data;
using System.Data.SqlClient;
namespace SQLLiteDataTransferUtility;

using System.Data;
using System.IO;
using System.Runtime.InteropServices;

class Program
{
    static string SQLConnection = "";
    static void Main(string[] args)
    {
        var builder = new ConfigurationBuilder();
        builder.SetBasePath(Directory.GetCurrentDirectory())
               .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

        IConfiguration config = builder.Build();
        SQLConnection = config["SQLConnectionString"].ToString();
        foreach (var folderPath in config["FilePath"].ToString().Split(","))
        {
            foreach (string file in Directory.EnumerateFiles(folderPath, "*.db"))
            {
                ReadData(file, "APPLOG");
                DeleteData(file, "APPLOG");
                ReadData(file, "KEYLOG");
                DeleteData(file, "KEYLOG");
                ReadData(file, "KEYLOG_RESULT");
                DeleteData(file, "KEYLOG_RESULT");
                ReadData(file, "USERLOG");
                DeleteData(file, "USERLOG");
                ReadData(file, "WEBLOG");
                DeleteData(file, "WEBLOG");
            }
        }

    }
    private static void ReadData(string Dbpath, string table)
    {
        using (var connection = new SqliteConnection("Data Source=" + Dbpath))
        {
            connection.Open();

            SqliteCommand cmd = (SqliteCommand)SqliteFactory.Instance.CreateCommand();
            cmd.CommandText = "SELECT * FROM " + table + ";";
            cmd.Connection = connection;

            DataTable dt;
            Console.WriteLine("Data Read Started from " + Dbpath + " and table is " + table + " ");
            using (SqliteDataReader dr = cmd.ExecuteReader())
            {
                do
                {
                    dt = new DataTable();
                    dt.BeginLoadData();
                    dt.Load(dr);
                    dt.EndLoadData();

                } while (!dr.IsClosed && dr.NextResult());
                Console.WriteLine("Data Read Completed from " + Dbpath + " and table is " + table + " -  Count is - " + dt.Rows.Count);

                InsertDataSQL(dt, table);
            }
        }

    }
    private static void InsertDataSQL(DataTable dt, string table)
    {
        try
        {
            Console.WriteLine("Insert into " + table + " Count is - " + dt.Rows.Count);

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string Query = string.Empty;
                if (table == "APPLOG") Query = "INSERT INTO APPLOG(HOST,[USER],TERMINAL,[SESSION],[EVENT],APPID,[TIME],[BINARY],CAPTION,DESCR,CLASS,CMD,SCREENSHOT) VALUES('" + dt.Rows[i][0].ToString().Replace("'", "''") + "','" + dt.Rows[i][1].ToString().Replace("'", "''") + "','" + dt.Rows[i][2].ToString().Replace("'", "''") + "','" + dt.Rows[i][3].ToString().Replace("'", "''") + "','" + dt.Rows[i][4].ToString().Replace("'", "''") + "','" + dt.Rows[i][5].ToString().Replace("'", "''") + "','" + dt.Rows[i][6].ToString().Replace("'", "''") + "','" + dt.Rows[i][7].ToString().Replace("'", "''") + "','" + dt.Rows[i][8].ToString().Replace("'", "''") + "','" + dt.Rows[i][9].ToString().Replace("'", "''") + "','" + dt.Rows[i][10].ToString().Replace("'", "''") + "','" + dt.Rows[i][11].ToString().Replace("'", "''") + "','" + dt.Rows[i][12].ToString().Replace("'", "''") + "')";
                else if (table == "KEYLOG") Query = "INSERT INTO KEYLOG(HOST,[USER],TERMINAL,[SESSION],APPID,[TIME],[NAME],CAPTION,KEYSTROKES,SCREENSHOT) VALUES('" + dt.Rows[i][0].ToString().Replace("'", "''") + "','" + dt.Rows[i][1].ToString().Replace("'", "''") + "','" + dt.Rows[i][2].ToString().Replace("'", "''") + "','" + dt.Rows[i][3].ToString().Replace("'", "''") + "','" + dt.Rows[i][4].ToString().Replace("'", "''") + "','" + dt.Rows[i][5].ToString().Replace("'", "''") + "','" + dt.Rows[i][6].ToString().Replace("'", "''") + "','" + dt.Rows[i][7].ToString().Replace("'", "''") + "','" + dt.Rows[i][8].ToString().Replace("'", "''") + "','" + dt.Rows[i][9].ToString().Replace("'", "''") + "')";
                else if (table == "KEYLOG_RESULT") Query = "INSERT INTO KEYLOG_RESULT(HOST,[USER],[TIME],CHANGED,[NAME],CAPTION,KEYSTROKES) VALUES('" + dt.Rows[i][0].ToString().Replace("'", "''") + "','" + dt.Rows[i][1].ToString().Replace("'", "''") + "','" + dt.Rows[i][2].ToString().Replace("'", "''") + "','" + dt.Rows[i][3].ToString().Replace("'", "''") + "','" + dt.Rows[i][4].ToString().Replace("'", "''") + "','" + dt.Rows[i][5].ToString().Replace("'", "''") + "','" + dt.Rows[i][6].ToString().Replace("'", "''") + "')";
                else if (table == "USERLOG") Query = "INSERT INTO USERLOG(Id,Host,[User],Terminal,[Session],[Event],[Time],Duration,AppId,[Binary],Descr,Caption,Screenshot) VALUES('" + dt.Rows[i][0].ToString().Replace("'", "''") + "','" + dt.Rows[i][1].ToString().Replace("'", "''") + "','" + dt.Rows[i][2].ToString().Replace("'", "''") + "','" + dt.Rows[i][3].ToString().Replace("'", "''") + "','" + dt.Rows[i][4].ToString().Replace("'", "''") + "','" + dt.Rows[i][5].ToString().Replace("'", "''") + "','" + dt.Rows[i][6].ToString().Replace("'", "''") + "','" + dt.Rows[i][7].ToString().Replace("'", "''") + "','" + dt.Rows[i][8].ToString().Replace("'", "''") + "','" + dt.Rows[i][9].ToString().Replace("'", "''") + "','" + dt.Rows[i][10].ToString().Replace("'", "''") + "','" + dt.Rows[i][11].ToString().Replace("'", "''") + "','" + dt.Rows[i][12].ToString().Replace("'", "''") + "')";
                else if (table == "WEBLOG") Query = "INSERT INTO WEBLOG(HOST,[USER],TERMINAL,[SESSION],[EVENT],APPID,[TIME],[URL],TITLE,BROWSER,SCREENSHOT) VALUES('" + dt.Rows[i][0].ToString().Replace("'", "''") + "','" + dt.Rows[i][1].ToString().Replace("'", "''") + "','" + dt.Rows[i][2].ToString().Replace("'", "''") + "','" + dt.Rows[i][3].ToString().Replace("'", "''") + "','" + dt.Rows[i][4].ToString().Replace("'", "''") + "','" + dt.Rows[i][5].ToString().Replace("'", "''") + "','" + dt.Rows[i][6].ToString().Replace("'", "''") + "','" + dt.Rows[i][7].ToString().Replace("'", "''") + "','" + dt.Rows[i][8].ToString().Replace("'", "''") + "','" + dt.Rows[i][9].ToString().Replace("'", "''") + "','" + dt.Rows[i][10].ToString().Replace("'", "''") + "')";

                //Console.WriteLine(Query);
                using (var con = new SqlConnection(SQLConnection))
                {
                    using (var com = new SqlCommand(Query, con))
                    {
                        con.Open();
                        com.ExecuteNonQuery();
                    }
                }
            }
            Console.WriteLine("Insert into " + table + " completed");

        }
        catch (SqliteException ex)
        {
            Console.WriteLine(ex.Message);
        }
    }
    private static void DeleteData(string Dbpath, string table)
    {
        var sql = "DELETE FROM " + table;
        try
        {
            // Open a new database connection
            using var connection = new SqliteConnection("Data Source=" + Dbpath);
            connection.Open();
            // Bind parameters values
            using var command = new SqliteCommand(sql, connection);
            // Execute the DELETE statement
            var rowDeleted = command.ExecuteNonQuery();
            Console.WriteLine("File Path is " + Dbpath);
            Console.WriteLine(table + " has been deleted successfully.");

        }
        catch (SqliteException ex)
        {
            Console.WriteLine(ex.Message);
        }
    }
}