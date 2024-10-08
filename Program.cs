
using Microsoft.Extensions.Configuration;
using Microsoft.Data;
using Microsoft.Data.Sqlite;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
namespace SQLLiteDataTransferUtility;

using System.Data;
using System.IO;
using System.Net;
using System.Runtime.InteropServices;

class Program
{
    static string SQLConnection = "";
    static IConfiguration config = null;
    static void Main(string[] args)
    {

        var builder = new ConfigurationBuilder();
        builder.SetBasePath(Directory.GetCurrentDirectory())
               .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

        config = builder.Build();
        WriteLog("Data Transfer tool - Started @" + DateTime.Now);
        SQLConnection = config["SQLConnectionString"].ToString();
        foreach (var folderPath in config["FilePath"].ToString().Split(","))
        {
            foreach (string file in Directory.EnumerateFiles(folderPath, "*.db"))
            {
                WriteLog("Reading APPLOG - Started @" + DateTime.Now + " From file" + file);
                ReadData(file, "APPLOG");
                WriteLog("Reading APPLOG - Completed @" + DateTime.Now + " From file" + file);
                WriteLog("Delete APPLOG - Started @" + DateTime.Now + " From file" + file);
                DeleteData(file, "APPLOG");
                WriteLog("Delete APPLOG - Completed @" + DateTime.Now + " From file" + file);
                WriteLog("Read KEYLOG - Started @" + DateTime.Now + " From file" + file);
                ReadData(file, "KEYLOG");
                WriteLog("Read KEYLOG - Completed @" + DateTime.Now + " From file" + file);
                WriteLog("Delete KEYLOG - Started @" + DateTime.Now + " From file" + file);
                DeleteData(file, "KEYLOG");
                WriteLog("Delete KEYLOG - Completed @" + DateTime.Now + " From file" + file);
                WriteLog("Read KEYLOG_RESULT - Started @" + DateTime.Now + " From file" + file);
                ReadData(file, "KEYLOG_RESULT");
                WriteLog("Read KEYLOG_RESULT - Completed @" + DateTime.Now + " From file" + file);
                WriteLog("Delete KEYLOG_RESULT - Started @" + DateTime.Now + " From file" + file);
                DeleteData(file, "KEYLOG_RESULT");
                WriteLog("Delete KEYLOG_RESULT - Completed @" + DateTime.Now + " From file" + file);
                WriteLog("Read USERLOG - Started @" + DateTime.Now + " From file" + file);

                ReadData(file, "USERLOG");

                WriteLog("Read USERLOG - Completed @" + DateTime.Now + " From file" + file);

                WriteLog("Delete USERLOG - Started @" + DateTime.Now + " From file" + file);
                DeleteData(file, "USERLOG");
                WriteLog("Delete USERLOG - Completed @" + DateTime.Now + " From file" + file);

                WriteLog("Read WEBLOG - Started @" + DateTime.Now + " From file" + file);

                ReadData(file, "WEBLOG");
                WriteLog("Read WEBLOG - Completed @" + DateTime.Now + " From file" + file);

                WriteLog("Delete WEBLOG - Started @" + DateTime.Now + " From file" + file);
                DeleteData(file, "WEBLOG");
                WriteLog("Delete WEBLOG - Completed @" + DateTime.Now + " From file" + file);
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
            WriteLog("Data Read Started from " + Dbpath + " and table is " + table + " ");
            using (SqliteDataReader dr = cmd.ExecuteReader())
            {
                do
                {
                    dt = new DataTable();
                    dt.BeginLoadData();
                    dt.Load(dr);
                    dt.EndLoadData();

                } while (!dr.IsClosed && dr.NextResult());
                WriteLog("Data Read Completed from " + Dbpath + " and table is " + table + " -  Count is - " + dt.Rows.Count);

                InsertDataSQL(dt, table);
            }
        }

    }
    private static void InsertDataSQL(DataTable dt, string table)
    {
        try
        {
            WriteLog("Insert into " + table + " Count is - " + dt.Rows.Count);

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string Query = string.Empty;
                if (table == "APPLOG") Query = "INSERT INTO APPLOG(HOST,[USER],TERMINAL,[SESSION],[EVENT],APPID,[TIME],[BINARY],CAPTION,DESCR,CLASS,CMD,SCREENSHOT) VALUES('" + dt.Rows[i][0].ToString().Replace("'", "''") + "','" + dt.Rows[i][1].ToString().Replace("'", "''") + "','" + dt.Rows[i][2].ToString().Replace("'", "''") + "','" + dt.Rows[i][3].ToString().Replace("'", "''") + "','" + dt.Rows[i][4].ToString().Replace("'", "''") + "','" + dt.Rows[i][5].ToString().Replace("'", "''") + "','" + dt.Rows[i][6].ToString().Replace("'", "''") + "','" + dt.Rows[i][7].ToString().Replace("'", "''") + "','" + dt.Rows[i][8].ToString().Replace("'", "''") + "','" + dt.Rows[i][9].ToString().Replace("'", "''") + "','" + dt.Rows[i][10].ToString().Replace("'", "''") + "','" + dt.Rows[i][11].ToString().Replace("'", "''") + "','" + dt.Rows[i][12].ToString().Replace("'", "''") + "')";
                else if (table == "KEYLOG") Query = "INSERT INTO KEYLOG(HOST,[USER],TERMINAL,[SESSION],APPID,[TIME],[NAME],CAPTION,KEYSTROKES,SCREENSHOT) VALUES('" + dt.Rows[i][0].ToString().Replace("'", "''") + "','" + dt.Rows[i][1].ToString().Replace("'", "''") + "','" + dt.Rows[i][2].ToString().Replace("'", "''") + "','" + dt.Rows[i][3].ToString().Replace("'", "''") + "','" + dt.Rows[i][4].ToString().Replace("'", "''") + "','" + dt.Rows[i][5].ToString().Replace("'", "''") + "','" + dt.Rows[i][6].ToString().Replace("'", "''") + "','" + dt.Rows[i][7].ToString().Replace("'", "''") + "','" + dt.Rows[i][8].ToString().Replace("'", "''") + "','" + dt.Rows[i][9].ToString().Replace("'", "''") + "')";
                else if (table == "KEYLOG_RESULT") Query = "INSERT INTO KEYLOG_RESULT(HOST,[USER],[TIME],CHANGED,[NAME],CAPTION,KEYSTROKES) VALUES('" + dt.Rows[i][0].ToString().Replace("'", "''") + "','" + dt.Rows[i][1].ToString().Replace("'", "''") + "','" + dt.Rows[i][2].ToString().Replace("'", "''") + "','" + dt.Rows[i][3].ToString().Replace("'", "''") + "','" + dt.Rows[i][4].ToString().Replace("'", "''") + "','" + dt.Rows[i][5].ToString().Replace("'", "''") + "','" + dt.Rows[i][6].ToString().Replace("'", "''") + "')";
                else if (table == "USERLOG") Query = "INSERT INTO USERLOG(Id,Host,[User],Terminal,[Session],[Event],[Time],Duration,AppId,[Binary],Descr,Caption,Screenshot) VALUES('" + dt.Rows[i][0].ToString().Replace("'", "''") + "','" + dt.Rows[i][1].ToString().Replace("'", "''") + "','" + dt.Rows[i][2].ToString().Replace("'", "''") + "','" + dt.Rows[i][3].ToString().Replace("'", "''") + "','" + dt.Rows[i][4].ToString().Replace("'", "''") + "','" + dt.Rows[i][5].ToString().Replace("'", "''") + "','" + dt.Rows[i][6].ToString().Replace("'", "''") + "','" + dt.Rows[i][7].ToString().Replace("'", "''") + "','" + dt.Rows[i][8].ToString().Replace("'", "''") + "','" + dt.Rows[i][9].ToString().Replace("'", "''") + "','" + dt.Rows[i][10].ToString().Replace("'", "''") + "','" + dt.Rows[i][11].ToString().Replace("'", "''") + "','" + dt.Rows[i][12].ToString().Replace("'", "''") + "')";
                else if (table == "WEBLOG") Query = "INSERT INTO WEBLOG(HOST,[USER],TERMINAL,[SESSION],[EVENT],APPID,[TIME],[URL],TITLE,BROWSER,SCREENSHOT) VALUES('" + dt.Rows[i][0].ToString().Replace("'", "''") + "','" + dt.Rows[i][1].ToString().Replace("'", "''") + "','" + dt.Rows[i][2].ToString().Replace("'", "''") + "','" + dt.Rows[i][3].ToString().Replace("'", "''") + "','" + dt.Rows[i][4].ToString().Replace("'", "''") + "','" + dt.Rows[i][5].ToString().Replace("'", "''") + "','" + dt.Rows[i][6].ToString().Replace("'", "''") + "','" + dt.Rows[i][7].ToString().Replace("'", "''") + "','" + dt.Rows[i][8].ToString().Replace("'", "''") + "','" + dt.Rows[i][9].ToString().Replace("'", "''") + "','" + dt.Rows[i][10].ToString().Replace("'", "''") + "')";

                using (var con = new SqlConnection(SQLConnection))
                {
                    using (var com = new SqlCommand(Query, con))
                    {
                        con.Open();
                        com.ExecuteNonQuery();
                    }
                }
            }
            WriteLog("Insert into " + table + " completed");

        }
        catch (SqliteException ex)
        {
            WriteLog(ex.Message);
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
            WriteLog("File Path is " + Dbpath);
            WriteLog(table + " has been deleted successfully.");

        }
        catch (SqliteException ex)
        {
            WriteLog(ex.Message);
        }
    }
    public static void WriteLog(string Message)
    {


        string directoryName = config["Log"].ToString();
        try
        {
            DateTime today = DateTime.Today;
            string str = string.Concat(directoryName, "\\", today.ToString("dd-MM-yyyyy"));
            if (!Directory.Exists(directoryName))
            {
                Directory.CreateDirectory(directoryName);
            }
            string str1 = string.Concat(str, ".txt");
            if (!File.Exists(str1))
            {
                File.Create(str1).Dispose();
            }
            if (File.Exists(str1))
            {
                using (StreamWriter streamWriter = File.AppendText(str1))
                {
                    streamWriter.Write("\r\nLog Entry :");
                    today = DateTime.Now;
                    streamWriter.Write("{0}", today.ToString(CultureInfo.InvariantCulture));
                    streamWriter.WriteLine(string.Concat("-:-", Message));
                    streamWriter.Flush();
                    streamWriter.Close();
                }
            }
        }
        catch (Exception exception)
        {
            WriteLog(exception.Message);
        }

    }
}