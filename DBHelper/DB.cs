using System;
using MySql.Data.MySqlClient;
using System.Data;
using System.Data.SqlClient;
using Oracle.DataAccess.Client;

namespace DBHelper
{
    public class DB
    {
        public string _conn { get; set; }
        public int _timeout { get; set; }

        /// <summary>
        /// SQLServer数据库操作数据
        /// </summary>
        /// <param name="SQLServer_sql"></param>
        public string SQLServerDBHandle(string SQLServer_sql)
        {
            SqlConnection sqlconn = new SqlConnection(_conn);
            sqlconn.Open();
            SqlCommand sqlcom = new SqlCommand(SQLServer_sql, sqlconn);
            SqlTransaction tx = sqlconn.BeginTransaction();
            sqlcom.Transaction = tx;
            sqlcom.CommandTimeout = _timeout;
            try
            {
                int result = sqlcom.ExecuteNonQuery();
                tx.Commit();
                if (result > 0)
                {
                    return "OK";
                }
                else
                {
                    return "NG";
                }
            }
            catch (SqlException me)
            {
                tx.Rollback();
                return me.Message;
            }
            finally
            {
                sqlconn.Close();
            }
        }

        /// <summary>
        /// SQLServer数据库操作数据
        /// </summary>
        /// <param name="Oracle_sql"></param>
        /// <returns></returns>
        public string MySQLDBHandle(string MySQL_sql)
        {
            MySqlConnection sqlconn = new MySqlConnection(_conn);
            sqlconn.Open();
            MySqlTransaction tx = sqlconn.BeginTransaction();
            MySqlCommand sqlcom = new MySqlCommand(MySQL_sql, sqlconn);
            sqlcom.Transaction = tx;
            sqlcom.CommandTimeout = _timeout;
            try
            {
                int result = sqlcom.ExecuteNonQuery();
                tx.Commit();
                if (result > 0)
                {
                    return "OK";
                }
                else
                {
                    return "NG";
                }
            }
            catch (MySqlException me)
            {
                tx.Rollback();
                return me.Message;
            }
            finally
            {
                sqlconn.Close();
            }
        }

        /// <summary>
        /// Oracle数据库操作数据
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public string OracleDBHandle(string Oracle_sql)
        {
            OracleConnection sqlconn = new OracleConnection(_conn);
            sqlconn.Open();
            OracleTransaction tx = sqlconn.BeginTransaction();
            OracleCommand sqlcom = new OracleCommand(Oracle_sql, sqlconn);
            sqlcom.Transaction = tx;
            sqlcom.CommandTimeout = _timeout;
            try
            {
                int result = sqlcom.ExecuteNonQuery();
                tx.Commit();
                if (result > 0)
                {
                    return "OK";
                }
                else
                {
                    return "NG";
                }
            }
            catch (OracleException me)
            {
                tx.Rollback();
                return me.Message;
            }
            finally
            {
                sqlconn.Close();
            }
        }

        /// <summary>
        /// 将查询出的数据变成Table,SQLServer数据库
        /// </summary>
        /// <param name="SQLServer_sql"></param>
        /// <returns></returns>
        public DataTable SQLServerGetDataTable(string SQLServer_sql)
        {
            using (SqlConnection conn = new SqlConnection(_conn))
            {
                DataTable dt = new DataTable();
                try
                {
                    conn.Open();
                    SqlCommand comm = new SqlCommand(SQLServer_sql, conn);
                    SqlDataAdapter sda = new SqlDataAdapter();
                    comm.CommandTimeout = _timeout;
                    sda.SelectCommand = comm;
                    sda.Fill(dt);
                    conn.Close();
                }
                catch (SqlException ex)
                {
                    throw new Exception(ex.Message);
                }
                finally
                {
                    conn.Close();
                }
                return dt;
            }
        }

        /// <summary>
        /// 将查询出的数据变成Table,MySQL数据库
        /// </summary>
        /// <param name="Oracle_sql"></param>
        /// <returns></returns>
        public DataTable MySQLGetDataTable(string MySQL_sql)
        {
            using (MySqlConnection conn = new MySqlConnection(_conn))
            {
                DataTable dt = new DataTable();
                try
                {
                    conn.Open();
                    MySqlCommand comm = new MySqlCommand(MySQL_sql, conn);
                    MySqlDataAdapter sda = new MySqlDataAdapter();
                    comm.CommandTimeout = _timeout;
                    sda.SelectCommand = comm;
                    sda.Fill(dt);
                    conn.Close();
                }
                catch (MySqlException ex)
                {
                    throw new Exception(ex.Message);
                }
                finally
                {
                    conn.Close();
                }
                return dt;
            }
        }

        /// <summary>
        /// 将查询出的数据变成Table,Oracle数据库
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public DataTable OracleGetDataTable(string Oracle_sql)
        {
            using (OracleConnection conn = new OracleConnection(_conn))
            {
                DataTable dt = new DataTable();
                try
                {
                    conn.Open();
                    OracleCommand comm = new OracleCommand(Oracle_sql, conn);
                    OracleDataAdapter sda = new OracleDataAdapter();
                    comm.CommandTimeout = _timeout;
                    sda.SelectCommand = comm;
                    sda.Fill(dt);
                    conn.Close();
                }
                catch (OracleException ex)
                {
                    throw new Exception(ex.Message);
                }
                finally
                {
                    conn.Close();
                }
                return dt;
            }
        }
    }
}
