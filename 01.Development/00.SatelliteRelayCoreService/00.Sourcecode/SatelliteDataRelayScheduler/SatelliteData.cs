using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Data.OracleClient;
using System.Text.RegularExpressions;
using System.Net;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Threading;
using System.Reflection;

// 3rd party library
using Renci.SshNet;
using Newtonsoft.Json.Linq;
using log4net;
using log4net.Config;

namespace SatelliteDataRelayScheduler
{
    public class SatelliteData
    {
        public Dictionary<string, SatelliteInfo> Satellite = new Dictionary<string, SatelliteInfo>();

        // TODO:: 별도의 설정파일에서 관리해야만 함.
        private string SATELLITE_FILE_PATH;
        private double MINIMUM_INTERVAL_LAST_UPDATE_MINUTES;

        private const string FTP_SERVER_TYPE = "1";
        private const string DB_SERVER_TYPE = "2";

        // db conneciton
        private string DBFILE_PATH;
        private SQLiteConnection metaDBConn;        // 주기적으로 connection 갱신필요.
        private OracleConnection customerDBConn;    // 주기적으로 connection 갱신필요. 1개의 접속정보만 가져옴. 

        public Dictionary<string, List<ServerInfo>> Server = new Dictionary<string, List<ServerInfo>>();

        // configuration file
        private string confFilePath;

        private static readonly ILog log = LogManager.GetLogger(typeof(Program));

        // Constructor
        public SatelliteData(JObject satelliteConfObj)
        {
            // set configuration data
            SATELLITE_FILE_PATH = satelliteConfObj["SATELLITE_FILE_PATH"].ToString();
            MINIMUM_INTERVAL_LAST_UPDATE_MINUTES = satelliteConfObj["MINIMUM_INTERVAL_LAST_UPDATE_MINUTES"].ToObject<double>();
            DBFILE_PATH = satelliteConfObj["DBFILE_PATH"].ToString();

            confFilePath = System.IO.Directory.GetCurrentDirectory() + @"\conf\satelliteScheduler_config.json";

            //db 접속
            if (connectDB() == true)
            {
                // get database data : TB_SETELLITE_INFO, TB_PRODUCT_LIST, TB_SERVER_LIST
                getDBData();
            }

            var repo = log4net.LogManager.CreateRepository(Assembly.GetEntryAssembly(), typeof(log4net.Repository.Hierarchy.Hierarchy));

            XmlConfigurator.ConfigureAndWatch(repo, new System.IO.FileInfo("conf/SatelliteLog4net.config"));
        }

        // initialize database connecton
        private Boolean connectDB()
        {
            // set DB data on ScheduleData            
            metaDBConn = new SQLiteConnection(DBFILE_PATH);
            try
            {
                metaDBConn.Open();
            }
            catch (SQLiteException sqle)
            {
                // Handle DB exception
                log.Fatal("metadata Database connection failed : " + sqle.Message);
                metaDBConn.Dispose();
            }
            finally
            {                
            }

            if (metaDBConn != null && metaDBConn.State == ConnectionState.Closed)
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        private void chkMetaDBConn()
        {
            if ( metaDBConn.State != ConnectionState.Open)
            {
                metaDBConn.Close();
                metaDBConn.Open();
            }
        }

        private void chkCustomerDBConn()
        {
            if (customerDBConn.State != ConnectionState.Open)
            {
                customerDBConn.Close();
                customerDBConn.Open();
            }
        }

        // get database Data
        private Boolean getDBData()
        {
            // set DB data on ScheduleData
            string strSQL;
            SQLiteCommand sqlCmd;
            SQLiteDataReader satelliteRd, productRd, serverRd;
            string satelliteId;
            string server_type;
            string strCustomDBConn;

            // initialize dictionary
            Satellite.Clear();
            Server.Clear();

            strSQL = "SELECT * FROM TB_SATELLITE_INFO";
            sqlCmd = new SQLiteCommand(strSQL, metaDBConn);
            satelliteRd = sqlCmd.ExecuteReader();
            sqlCmd.Dispose();

            // 1) TB_SETELLITE_INFO 테이블 read 후 저장.
            while (satelliteRd.Read())
            {
                //Console.WriteLine(rd["SATELLITE"]);
                // add satellite information
                satelliteId = satelliteRd["SATELLITE"].ToString().Trim();

                Satellite.Add(satelliteId, new SatelliteInfo(satelliteRd));

                strSQL = "SELECT * FROM TB_PRODUCT_LIST WHERE SATELLITE = @satellite";
                sqlCmd = new SQLiteCommand(strSQL, metaDBConn);
                sqlCmd.Parameters.AddWithValue("@satellite", satelliteId);

                productRd = sqlCmd.ExecuteReader();

                // 2) 각 SATELLITE 별 PRODUCT 정보 읽어와서 저장.
                while (productRd.Read())
                {
                    // add product information
                    if (Satellite.ContainsKey(satelliteId) == true)
                    {
                        Satellite[satelliteId].SetProduct(productRd);
                    }
                }
                productRd.Close();
            }
            satelliteRd.Close();

            // 3) TB_SERVER_LIST 테이블(FTP, DB) read 후 저장.
            strSQL = "SELECT * FROM TB_SERVER_LIST WHERE SERVER_TYPE IN ( '1', '2')";
            sqlCmd = new SQLiteCommand(strSQL, metaDBConn);
            serverRd = sqlCmd.ExecuteReader();
            sqlCmd.Dispose();

            while (serverRd.Read())
            {
                server_type = serverRd["SERVER_TYPE"].ToString();
                if (Server.ContainsKey(server_type) == false)
                {
                    Server.Add(server_type, new List<ServerInfo>());
                }
                Server[server_type].Add(new ServerInfo(serverRd));
            }

            // 4) connect customer database server : using first connection information
            if ( Server.ContainsKey(DB_SERVER_TYPE) )
            {
                try
                {
                    // 아래와 같이 수행하면 ora-01005 bug가 계속 발생하므로 문자열 자체를 하드코딩 하도록 임시조치
                    /*
                    var connStr = new OracleConnectionStringBuilder()
                    {                        
                        UserID=Server[DB_SERVER_TYPE][0].Userid,
                        Unicode=true,
                        PersistSecurityInfo=true,
                        Password= Server[DB_SERVER_TYPE][0].Password,
                        DataSource = @"(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = " + Server[DB_SERVER_TYPE][0].Conn_Addr + @")(PORT = " + Server[DB_SERVER_TYPE][0].Conn_portno + @")))(CONNECT_DATA =(SID  = " + Server[DB_SERVER_TYPE][0].DBSid + @")))"
                    }.ConnectionString;
                    */
                    string connStr =
                        "Data Source=\"(DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = " + Server[DB_SERVER_TYPE][0].Conn_Addr
                        + ")(PORT = " + Server[DB_SERVER_TYPE][0].Conn_portno
                        + " )))(CONNECT_DATA = (SID = " + Server[DB_SERVER_TYPE][0].DBSid
                        + ")))\";Persist Security Info=True;Unicode=True;"
                        + "User ID=" + Server[DB_SERVER_TYPE][0].Userid
                        + ";Password=" + Server[DB_SERVER_TYPE][0].Password;

                    customerDBConn = new OracleConnection(connStr);
                    customerDBConn.Open();

                    log.Info("oracle connecting success...");
                }
                catch (Exception e)
                {
                    log.Error("exception messages : " + e.Message);
                }

                return true;
            }
            else
            {
                return false;
            }

        }

        // get ProductInfo
        public List<ProductInfo> GetProduct(string satelliteId)
        {
            return Satellite[satelliteId].GetProductInfo();
        }

        // get list of target satellite
        //  -> next_reserve_time과 현재 시간의 차이가 5초 이하일 경우 발동
        public List<string> GetTargetSatellite()
        {
            List<string> TargetSatellite = new List<string>();
            DateTime currentTime;
            DateTime nextReserveTime;
            double minTimeInterval = -1;

            TimeSpan timeDiff;

            currentTime = DateTime.ParseExact(DateTime.Now.ToString("HHmmss"), "HHmmss", null);

            foreach (var pair in Satellite)
            {
                nextReserveTime = DateTime.ParseExact(pair.Value.NextReserveTime, "HHmmss", null);
                timeDiff = nextReserveTime - currentTime;

                if ( timeDiff.TotalSeconds <= 5 && timeDiff.TotalSeconds >= 0 )
                {
                    TargetSatellite.Add(pair.Key);
                }
            }

            return TargetSatellite;
        }


        // get min time interval between current time and next reserve time
        // return second(double)
        public double GetMinTimeInterval()
        {
            DateTime currentTime;
            DateTime nextReserveTime;
            double minTimeInterval = -1;

            TimeSpan timeDiff;

            currentTime = DateTime.ParseExact(DateTime.Now.ToString("HHmmss"), "HHmmss", null);
        
            foreach (var pair in Satellite)
            {
                nextReserveTime = DateTime.ParseExact(pair.Value.NextReserveTime, "HHmmss", null);
                timeDiff = nextReserveTime - currentTime;

                if (timeDiff.TotalSeconds > 0)
                {
                    if (minTimeInterval < 0 || timeDiff.TotalSeconds < minTimeInterval)
                    {
                        minTimeInterval = timeDiff.TotalSeconds;
                    }
                }
            }

            return minTimeInterval;
        }

        // get SatelliteInfo
        public SatelliteInfo GetSatelliteInfo(string satelliteId)
        {
            return Satellite[satelliteId];
        }

        // relay satellite data
        /* non-threading version
        public void RelaySatellite(string satelliteId)
        {
            List<ProductInfo> Product = Satellite[satelliteId].GetProductInfo();
            string regex;
            TimeSpan lastUpdateTimeDiff;

            // get seqno
            long lastSeqNo = 0;
            //            Boolean getSeqno = false;            

            // 1) 대상 파일 확인 : Product 단위로 확인
            if (System.IO.Directory.Exists(SATELLITE_FILE_PATH))
            {
                System.IO.DirectoryInfo dir_info = new System.IO.DirectoryInfo(SATELLITE_FILE_PATH);

                for ( int i = 0 ; i < Product.Count; i++ )
                {                    
                    regex = Product[i].FlienmFormat;
                    //                    getSeqno = false;                    

                    // 대상 파일 선출(전송중인 파일은 반드시 제외되어야 함. DB로 확인해야 함)
                    foreach (var file_info in dir_info.GetFileSystemInfos())
                    {                        
                        // 정규식 확인
                        if ( Regex.IsMatch(file_info.Name, regex) )
                        {                            
                            // 최종 수정 시간 확인 : 5분 초과 파일만 가져오도록 함.
                            lastUpdateTimeDiff = DateTime.Now - file_info.LastWriteTime;

                            // timespan의 TotalMinutes 5분을 넘는지 확인해야 함.
                            if ( lastUpdateTimeDiff.TotalMinutes > MINIMUM_INTERVAL_LAST_UPDATE_MINUTES )
                            {
                                // TB_IDENTITY_LIST의 PK는 SEQ이므로 파일 단위로 SEQ 관리해야만 함.(TRANSACTION으로 잡아서 처리)
                                lastSeqNo = setSeqNo("TB_IDENTITY_LIST", "");

                                Console.WriteLine("satellite [" + satelliteId + "] relaySatelliteData start");

                                relaySatelliteData(satelliteId, Product[i], file_info.Name, file_info.FullName, lastSeqNo);
                            }
                        }                        
                    }

                }                
            }

            // update next reserve time : memory & db
            if ( updatetNextReserveTime(satelliteId) != true )
            {
                // logging
                Console.WriteLine("updatetNextReserveTime failed");
            }
        }
        */

        public void RelaySatellite(string satelliteId)
        {
            List<ProductInfo> Product = Satellite[satelliteId].GetProductInfo();
            string regex;
            TimeSpan lastUpdateTimeDiff;

            // get seqno
            long lastSeqNo = 0;

            // 1) 대상 파일 확인 : Product 단위로 확인
            if (System.IO.Directory.Exists(SATELLITE_FILE_PATH))
            {
                System.IO.DirectoryInfo dir_info = new System.IO.DirectoryInfo(SATELLITE_FILE_PATH);

                for (int i = 0; i < Product.Count; i++)
                {
                    regex = Product[i].FlienmFormat;

                    // 대상 파일 선출(전송중인 파일은 반드시 제외되어야 함. DB로 확인해야 함)
                    foreach (var file_info in dir_info.GetFileSystemInfos())
                    {
                        // 정규식 확인
                        if (Regex.IsMatch(file_info.Name, regex))
                        {
                            // 최종 수정 시간 확인 : 5분 초과 파일만 가져오도록 함.
                            lastUpdateTimeDiff = DateTime.Now - file_info.LastWriteTime;

                            // timespan의 TotalMinutes 5분을 넘는지 확인해야 함.
                            if (lastUpdateTimeDiff.TotalMinutes > MINIMUM_INTERVAL_LAST_UPDATE_MINUTES)
                            {
                                // TB_IDENTITY_LIST의 PK는 SEQ이므로 파일 단위로 SEQ 관리해야만 함.(TRANSACTION으로 잡아서 처리)
                                lastSeqNo = setSeqNo("TB_IDENTITY_LIST", "");

                                log.Info("satellite [" + satelliteId + "] relaySatelliteData start");

                                relaySatelliteData(satelliteId, Product[i], file_info.Name, file_info.FullName, lastSeqNo);
                            }
                        }
                    }

                }
            }

            // update next reserve time : memory & db
            if (updatetNextReserveTime(satelliteId) != true)
            {
                log.Error("updatetNextReserveTime failed");
            }
        }

        // relay satellite 
        private Boolean relaySatelliteData(string satelliteId, ProductInfo productInfo, string identifier, string sendfileFullPath, long lastSeqNo)
        {
            // Set data about identifier
            IdentifierInfo identifierInfo = new IdentifierInfo(sendfileFullPath, productInfo.DataType, productInfo.FtpFileDir);

            // 0) validate whether relay processing file
            if ( chkRelayStart(identifier) != true )
            {
                return false;
            }

            log.Info("identifier[" + identifier + "] TB_PRODUCT_HISTORY(Metadata DB) INSERT START");
            // 1) INSERT TB_PRODUCT_HISTORY : 20%
            if (insertProductHistory(satelliteId, productInfo, identifierInfo, lastSeqNo) != true )
            {
                return false;
            }
            else
            {
                updateProductHistory(lastSeqNo, 20, "0");
            }
            log.Info("identifier[" + identifier + "] TB_PRODUCT_HISTORY(Metadata DB) INSERT END");

            // 2) FTP 파일 전송 : 40%(내부에서 update), 60%(내부 check logic 후 여기서 update.)
            log.Info("identifier[" + identifier + "] FTP file sending START");
            if (sendSFTP(satelliteId, identifierInfo, sendfileFullPath, lastSeqNo) != true)
            {
                return false;
            }
            else
            {
                updateProductHistory(lastSeqNo, 60, "0");
            }
            log.Info("identifier[" + identifier + "] FTP file sending END");

            log.Info("identifier[" + identifier + "] TB_IDENTITY_LIST(remote db) INSERT START");
            // 4) INSERT TB_IDENTITY_LIST : 80%
            if (insertIdentityList(lastSeqNo) != true)
            {
                return false;
            }
            else
            {
                updateProductHistory(lastSeqNo, 80, "0");
            }
            log.Info("identifier[" + identifier + "] TB_IDENTITY_LIST(remote db) INSERT END");


            log.Info("identifier[" + identifier + "] remove completed file START");
            // 5) DELETE FILE : 100%
            if (removeCompletedIdentity(sendfileFullPath, lastSeqNo) != true)
            {
                return false;
            }
            else
            {
                updateProductHistory(lastSeqNo, 100, "1");
            }
            log.Info("identifier[" + identifier + "] remove completed file END");

            return true;
        }

        private Boolean updatetNextReserveTime(string satelliteId)
        {
            string updateSQL;
            SQLiteCommand sqlCmd;

            updateSQL = "UPDATE TB_SATELLITE_INFO SET NEXT_RESERVE_TIME = @nextReserveTime WHERE SATELLITE = @satelliteId";

            chkMetaDBConn();

            using (var transaction = metaDBConn.BeginTransaction())
            {
                try
                {
                    // UPDATE sequence number
                    sqlCmd = new SQLiteCommand(updateSQL, metaDBConn);

                    sqlCmd.Parameters.AddWithValue("@nextReserveTime", Satellite[satelliteId].SetNextReserveTime());
                    sqlCmd.Parameters.AddWithValue("@satelliteId", satelliteId);

                    sqlCmd.ExecuteNonQuery();
                    sqlCmd.Dispose();

                    transaction.Commit();

                }
                catch (Exception e)
                {
                    log.Error("satellite [" + satelliteId + "] updatetNextReserveTime exception message : " + e.Message);
                    transaction.Rollback();
                    return false;
                }
                finally
                {
                }
            }

            return true;
        }
        
        private long setSeqNo(string seqType1, string seqType2)
        {            
            string updateSQL;
            string selectSQL;
            SQLiteCommand sqlCmd;
            SQLiteDataReader seqRd;

            long currentSeq = -1;
            
            updateSQL = "INSERT OR REPLACE INTO TB_SEQ(SEQ_TYPE1, SEQ_TYPE2, LAST_SEQ) VALUES (@seqType1, @seqType2, (SELECT ifnull(MAX(LAST_SEQ) + 1, 0) FROM TB_SEQ WHERE SEQ_TYPE1 = @seqType1 AND SEQ_TYPE2 = @seqType2) )";

            selectSQL = "SELECT LAST_SEQ FROM TB_SEQ WHERE SEQ_TYPE1 = @seqType1 AND SEQ_TYPE2 = @seqType2";

            chkMetaDBConn();
            
            using (var transaction = metaDBConn.BeginTransaction())
            {
                try
                {
                    // UPDATE sequence number
                    sqlCmd = new SQLiteCommand(updateSQL, metaDBConn);

                    sqlCmd.Parameters.AddWithValue("@seqType1", seqType1);
                    sqlCmd.Parameters.AddWithValue("@seqType2", seqType2);

                    sqlCmd.ExecuteNonQuery();
                    sqlCmd.Dispose();

                    // Get current sequence number
                    sqlCmd = new SQLiteCommand(selectSQL, metaDBConn);

                    sqlCmd.Parameters.AddWithValue("@seqType1", seqType1);
                    sqlCmd.Parameters.AddWithValue("@seqType2", seqType2);

                    seqRd = sqlCmd.ExecuteReader();
                    sqlCmd.Dispose();

                    if (seqRd.Read())
                    {
                        currentSeq = (long)(seqRd["LAST_SEQ"]);
                    }

                    seqRd.Close();

                    transaction.Commit();

                }
                catch (Exception e)
                {
                    log.Error("[" + seqType1 + "," + seqType2 + "] setSeqNo exception message : " + e.Message);
                    transaction.Rollback();
                    return -1;
                }
                finally
                {
                }

            }

            return currentSeq;
        }

        // SEQ가 KEY이지만, IDENTIFIER도 KEY로 간주하고 존재여부 확인
        private Boolean chkRelayStart(string identifier)
        {
            string strSQL;
            SQLiteCommand sqlCmd;
            SQLiteDataReader productHistoryRd;

            chkMetaDBConn();

            strSQL = "SELECT IDENTIFIER ISEXIST FROM TB_PRODUCT_HISTORY WHERE IDENTIFIER = @identifier";
            sqlCmd = new SQLiteCommand(strSQL, metaDBConn);

            sqlCmd.Parameters.AddWithValue("@identifier", identifier);

            productHistoryRd = sqlCmd.ExecuteReader();
            sqlCmd.Dispose();

            if (productHistoryRd.Read())
            {                
                log.Error("identifier [" + identifier + "] already exist on DB");
                productHistoryRd.Close();
                return false; 
            }
            else
            {
                productHistoryRd.Close();
                return true;
            }
        }
        private Boolean insertProductHistory(string satelliteId, ProductInfo productInfo, IdentifierInfo identifierInfo, long seqno)
        {
            string insertSQL;
            SQLiteCommand sqlCmd;

            insertSQL = "INSERT INTO TB_PRODUCT_HISTORY ( SEQ, SATELLITE, DATA_GBN, DATA_AN_GBN, DATA_TYPE, IDENTIFIER, START_TIME, END_TIME, RELAY_STATUS, SURVEY_DATE, COORD_UL, COORD_LR, PIXEL_ROW, PIXEL_COL, DATA_FORMAT, PROJECTION, QUICK_LOOK, RESOLUTION, FILE_SIZE, FILE_STATUS, FILE_PATH, REG_DATE, MOUNT_POINT, DATA_OPEN, SURVEY_TIME, OUTPUT_SATELLITE ) "
                + "SELECT @seqno, SATELLITE, DATA_GBN, DATA_AN_GBN, DATA_TYPE, @identifier, @start_time, '', '0', @surveyDate, COORD_UL, COORD_LR, PIXEL_ROW, PIXEL_COL, @dataFormat, PROJECTION, QUICK_LOOK, RESOLUTION, @fileSize, FILE_STATUS, @filePath, @regDate, MOUNT_POINT, DATA_OPEN, @surveyTime, OUTPUT_SATELLITE "
                + "FROM TB_PRODUCT_LIST WHERE SATELLITE = @satelliteId AND DATA_GBN = @dataGbn AND DATA_AN_GBN = @dataAnGbn AND DATA_TYPE = @dataType";

            chkMetaDBConn();

            using (var transaction = metaDBConn.BeginTransaction())
            {
                try
                {
                    // UPDATE sequence number
                    sqlCmd = new SQLiteCommand(insertSQL, metaDBConn);

                    // used SELECT phrase
                    sqlCmd.Parameters.AddWithValue("@seqno", seqno);
                    sqlCmd.Parameters.AddWithValue("@identifier", identifierInfo.IdentifierNm);
                    sqlCmd.Parameters.AddWithValue("@start_time", DateTime.Now.ToString("yyyyMMddHHmmss"));
                    sqlCmd.Parameters.AddWithValue("@surveyDate", identifierInfo.SurveyDate);
                    sqlCmd.Parameters.AddWithValue("@dataFormat", identifierInfo.DataFormat);
                    sqlCmd.Parameters.AddWithValue("@fileSize", identifierInfo.FileSize);
                    sqlCmd.Parameters.AddWithValue("@filePath", identifierInfo.OutputFilePath);
                    sqlCmd.Parameters.AddWithValue("@regDate", identifierInfo.RegDate);
                    sqlCmd.Parameters.AddWithValue("@surveyTime", identifierInfo.SurveyTime);

                    // used WHERE phrase
                    sqlCmd.Parameters.AddWithValue("@satelliteId", satelliteId);
                    sqlCmd.Parameters.AddWithValue("@dataGbn", productInfo.DataGbn);
                    sqlCmd.Parameters.AddWithValue("@dataAnGbn", productInfo.DataAnGbn);
                    sqlCmd.Parameters.AddWithValue("@dataType", productInfo.DataType);

                    sqlCmd.ExecuteNonQuery();
                    sqlCmd.Dispose();

                    transaction.Commit();

                }
                catch (Exception e)
                {
                    log.Error("insertProductHistory exception message : " + e.Message);
                    transaction.Rollback();
                    return false;
                }
                finally
                {
                }

            }


            return true;
        }
        private Boolean updateProductHistory(long seqno, long progress, string relay_status)
        {
            string updateSQL;
            SQLiteCommand sqlCmd;

            updateSQL = "UPDATE TB_PRODUCT_HISTORY SET PROGRESS = @progress, RELAY_STATUS = @relay_status, END_TIME = @end_time WHERE SEQ = @seqno";

            chkMetaDBConn();

            using (var transaction = metaDBConn.BeginTransaction())
            {
                try
                {
                    // UPDATE progress
                    sqlCmd = new SQLiteCommand(updateSQL, metaDBConn);

                    sqlCmd.Parameters.AddWithValue("@progress", progress);
                    sqlCmd.Parameters.AddWithValue("@relay_status", relay_status);

                    // relay 성공시 성공시간 기록
                    if ( relay_status[0] == '1' )
                    {
                        sqlCmd.Parameters.AddWithValue("@end_time", DateTime.Now.ToString("yyyyMMddHHmmss"));
                    }
                    else
                    {
                        sqlCmd.Parameters.AddWithValue("@end_time", "");
                    }
                        
                    sqlCmd.Parameters.AddWithValue("@seqno", seqno);

                    sqlCmd.ExecuteNonQuery();
                    sqlCmd.Dispose();

                    transaction.Commit();

                }
                catch (SQLiteException sqle)
                {
                    log.Error("[" + seqno.ToString() + "," + progress.ToString() + "," + relay_status + "] updateProductHistory failed : " + sqle.Message);
                    transaction.Rollback();
                    return false;
                }
                finally
                {
                }

            }

            return true;
        }
        /*
        private Boolean sendFTP(string satelliteId, IdentifierInfo identifierInfo, string sendfileFullPath, long seqno)
        {
            string ftpAddress;
            string portno;
            string userid;
            string password;

            FtpWebRequest reqFTP;

            byte[] sendData;
            StreamReader reader;

            if ( Server.ContainsKey(FTP_SERVER_TYPE) )
            {
                // 입력파일을 바이트 배열로 읽음                    
                reader = new StreamReader(sendfileFullPath);
                sendData = Encoding.UTF8.GetBytes(reader.ReadToEnd());                

                for ( int i = 0; i < Server[FTP_SERVER_TYPE].Count ; i++ )
                {
                    ftpAddress = Server[FTP_SERVER_TYPE][i].Conn_Addr;
                    portno = Server[FTP_SERVER_TYPE][i].Conn_portno;
                    userid = Server[FTP_SERVER_TYPE][i].Userid;
                    password = Server[FTP_SERVER_TYPE][i].Password;

                    // connection by specific port number
                    if ( portno.Length > 0)
                    {
                        ftpAddress = ftpAddress + ":" + portno;
                    }
                    if ( identifierInfo.OutputFilePath.Length > 0 )
                    {
                        ftpAddress = ftpAddress + "/" + identifierInfo.OutputFilePath;
                    }

                    reqFTP = (FtpWebRequest)WebRequest.Create(ftpAddress);
                    reqFTP.Credentials = new NetworkCredential(userid, password); // 쓰기 권한이 있는 FTP 사용자 로그인 지정
                    reqFTP.UsePassive = false;

                    // 1) check folder exist
                    try
                    {                        
                        reqFTP.Method = WebRequestMethods.Ftp.ListDirectory;                        

                        using (FtpWebResponse respFTP = (FtpWebResponse)reqFTP.GetResponse())
                        {
                            // folder exists
                        }
                    }
                    catch (WebException chkEx)
                    {
                        if (chkEx.Response != null)
                        {
                            using (FtpWebResponse chkResp = (FtpWebResponse)chkEx.Response)
                            {
                                if (chkResp.StatusCode == FtpStatusCode.ActionNotTakenFileUnavailable)
                                {
                                    Console.WriteLine("target folder not exist");
                                    try
                                    {
                                        FtpWebRequest creatDirFTP = reqFTP;
                                        // Directory not found. So create folder
                                        creatDirFTP.Method = WebRequestMethods.Ftp.MakeDirectory;

                                        using (FtpWebResponse creatDirResp = (FtpWebResponse)creatDirFTP.GetResponse())
                                        {
                                        }
                                    }
                                    catch (WebException CreateEx)
                                    {
                                        using (FtpWebResponse createFTP = (FtpWebResponse)CreateEx.Response)
                                        {
                                            // FTP 결과 상태 출력
                                            Console.WriteLine("FTP directory create failed : {0}, {1}", chkResp.StatusDescription, chkResp.StatusDescription);
                                        }

                                        return false;
                                    }
                                }
                                else
                                {
                                    Console.WriteLine("FTP directory not exist. It is hard to find fail reason : " + chkResp.StatusDescription
                                        );
                                }
                            }
                        }
                    }
                    finally
                    {

                    }

                    // 2) upload ftp file
                    try
                    {                        
                        // FTP 업로드한다는 것을 표시
                        reqFTP.Method = WebRequestMethods.Ftp.UploadFile;                        
                        reqFTP.ContentLength = sendData.Length;

                        // FTP upload
                        using (Stream reqStream = reqFTP.GetRequestStream())
                        {
                            reqStream.Write(sendData, 0, sendData.Length);
                        }

                        updateProductHistory(seqno, 40, "0");

                        // Check FTP upload success
                        using (FtpWebResponse respFTP = (FtpWebResponse)reqFTP.GetResponse())
                        {
                            // FTP 결과 상태 출력
                            Console.WriteLine("FTP file Upload success : {0}, {1}", respFTP.StatusDescription, respFTP.StatusDescription);
                        }
                    }
                    catch (WebException uploadEx)
                    {
                        using (FtpWebResponse uploadResp = (FtpWebResponse)uploadEx.Response)
                        {
                            // FTP 결과 상태 출력
                            Console.WriteLine("FTP file Upload failed : {0}, {1}", uploadResp.StatusDescription, uploadResp.StatusDescription);
                        }

                        return false;
                    }
                    finally
                    {

                    }
                }

                return true;
            }
            else
            {
                // logging not enrolled ftp server
                return false;
            }
        }
        */
        private Boolean sendSFTP(string satelliteId, IdentifierInfo identifierInfo, string sendfileFullPath, long seqno)
        {
            string ftpAddress;
            int portno;
            string userid;
            string password;
            string remoteUploadPath;

            if (Server.ContainsKey(FTP_SERVER_TYPE))
            {
                for (int i = 0; i < Server[FTP_SERVER_TYPE].Count; i++)
                {
                    ftpAddress = Server[FTP_SERVER_TYPE][i].Conn_Addr;
                    portno = Int32.Parse(Server[FTP_SERVER_TYPE][i].Conn_portno);
                    userid = Server[FTP_SERVER_TYPE][i].Userid;
                    password = Server[FTP_SERVER_TYPE][i].Password;

                    if ( identifierInfo.OutputFilePath[identifierInfo.OutputFilePath.Length-1] != '/' )
                    {
                        remoteUploadPath = identifierInfo.OutputFilePath + "/";
                    }
                    else
                    {
                        remoteUploadPath = identifierInfo.OutputFilePath;
                    }
                    

                    try
                    {
                        using (SftpClient client = new SftpClient(ftpAddress, portno, userid, password))
                        {
                            Console.WriteLine("sftp connecting start" + sendfileFullPath);
                            client.Connect();
                            Console.WriteLine("sftp connecting end");

                            client.CreateDirectory(remoteUploadPath);
                            client.ChangeDirectory(remoteUploadPath);

                            using (FileStream fs = new FileStream(sendfileFullPath, FileMode.Open))
                            {
                                client.BufferSize = 4 * 1024;
                                client.UploadFile(fs, identifierInfo.IdentifierNm);
                            }

                            // set progress to 40%
                            updateProductHistory(seqno, 40, "0");

                            if ( client.Exists(identifierInfo.IdentifierNm) )
                            {
                                // set progress to 60%
                                updateProductHistory(seqno, 60, "0");
                            }
                            else
                            {
                                // set relay_status to failed
                                updateProductHistory(seqno, 40, "2");
                                return false;
                            }
                            
                        }
                    }
                    catch ( Exception e)
                    {
                        log.Error("identifier[" + identifierInfo.IdentifierNm + " sendSFTP failed. messages : " + e.Message);
                        return false;
                    }
                    
                }
            }
            return true;        
        }


        private Boolean insertIdentityList(long lastSeqNo)
        {
            string productHistorySQL;
            SQLiteCommand productHistoryCmd;
            SQLiteDataReader productHistoryRd;

            string insertSQL;
            OracleCommand sqlCmd;

            try
            {
                // 1) get data from TB_PRODUCT_HISTORY table on metadata database(sqlite)
                chkMetaDBConn();

                // multi row 조회 방지를 위해서 rownum 추가
                productHistorySQL = "SELECT * FROM TB_PRODUCT_HISTORY WHERE SEQ = @seqno";
                productHistoryCmd = new SQLiteCommand(productHistorySQL, metaDBConn);
                productHistoryCmd.Parameters.AddWithValue("@seqno", lastSeqNo);

                productHistoryRd = productHistoryCmd.ExecuteReader();
                productHistoryCmd.Dispose();

                if (!productHistoryRd.Read())
                {
                    log.Error("TB_PRODUCT_HISTORY data not exists. seqno[" + lastSeqNo.ToString() + "]");
                    productHistoryRd.Close();
                    return false;
                }

                // 2) insert data to TB_IDENTITY_LIST on customer database(oracle) 
                chkCustomerDBConn();

                insertSQL = "INSERT INTO TB_IDENTITY_LIST(SEQ, IDENTIFIER, SURVEY_DATE, COORD_UL, COORD_LR, PIXEL_ROW, PIXEL_COL, DATA_TYPE, DATA_FORMAT, PROJECTION, QUICK_LOOK, DATA_GBN, DATA_AN_GBN, SATELLITE, RESOLUTION, FILE_SIZE, FILE_STATUS, FILE_PATH, REG_DATE, MOUNT_POINT, DATA_OPEN, SURVEY_TIME ) VALUES "
                            + "( :SEQ, :IDENTIFIER, :SURVEY_DATE, :COORD_UL, :COORD_LR, :PIXEL_ROW, :PIXEL_COL, :DATA_TYPE, :DATA_FORMAT, :PROJECTION, :QUICK_LOOK, :DATA_GBN, :DATA_AN_GBN, :SATELLITE, :RESOLUTION, :FILE_SIZE, :FILE_STATUS, :FILE_PATH, :REG_DATE, :MOUNT_POINT, :DATA_OPEN, :SURVEY_TIME ) ";

                sqlCmd = new OracleCommand(insertSQL, customerDBConn);

                sqlCmd.CommandText = insertSQL;

                sqlCmd.Parameters.Add("SEQ", Convert.ToDecimal((long)(productHistoryRd["SEQ"])));
                sqlCmd.Parameters.Add("IDENTIFIER", productHistoryRd["IDENTIFIER"]);
                sqlCmd.Parameters.Add("SURVEY_DATE", productHistoryRd["SURVEY_DATE"]);
                sqlCmd.Parameters.Add("COORD_UL", productHistoryRd["COORD_UL"]);
                sqlCmd.Parameters.Add("COORD_LR", productHistoryRd["COORD_LR"]);
                sqlCmd.Parameters.Add("PIXEL_ROW", Convert.ToDecimal((long)(productHistoryRd["PIXEL_ROW"])));
                sqlCmd.Parameters.Add("PIXEL_COL", Convert.ToDecimal((long)(productHistoryRd["PIXEL_COL"])));
                sqlCmd.Parameters.Add("DATA_TYPE", productHistoryRd["DATA_TYPE"]);
                sqlCmd.Parameters.Add("DATA_FORMAT", productHistoryRd["DATA_FORMAT"]);
                sqlCmd.Parameters.Add("PROJECTION", productHistoryRd["PROJECTION"]);
                sqlCmd.Parameters.Add("QUICK_LOOK", productHistoryRd["QUICK_LOOK"]);
                sqlCmd.Parameters.Add("DATA_GBN", productHistoryRd["DATA_GBN"]);
                sqlCmd.Parameters.Add("DATA_AN_GBN", productHistoryRd["DATA_AN_GBN"]);
                sqlCmd.Parameters.Add("SATELLITE", productHistoryRd["OUTPUT_SATELLITE"]);
                sqlCmd.Parameters.Add("RESOLUTION", productHistoryRd["RESOLUTION"]);
                sqlCmd.Parameters.Add("FILE_SIZE", productHistoryRd["FILE_SIZE"]);
                sqlCmd.Parameters.Add("FILE_STATUS", productHistoryRd["FILE_STATUS"]);
                sqlCmd.Parameters.Add("FILE_PATH", productHistoryRd["FILE_PATH"]);
                sqlCmd.Parameters.Add("REG_DATE", productHistoryRd["REG_DATE"]);
                sqlCmd.Parameters.Add("MOUNT_POINT", productHistoryRd["MOUNT_POINT"]);
                sqlCmd.Parameters.Add("DATA_OPEN", productHistoryRd["DATA_OPEN"]);
                sqlCmd.Parameters.Add("SURVEY_TIME", productHistoryRd["SURVEY_TIME"]);

                productHistoryRd.Close();

                using (OracleTransaction transaction = customerDBConn.BeginTransaction(IsolationLevel.ReadCommitted))
                {
                    sqlCmd.Transaction = transaction;
                    sqlCmd.ExecuteNonQuery();
                    transaction.Commit();
                }
            }
            catch (Exception e)
            {
                log.Error("seqno [" + lastSeqNo.ToString() + "] insertIdentityList failed. messages : " + e.Message);
                return false;
            }
            

            return true;
        }
        private Boolean removeCompletedIdentity(string sendfileFullPath, long lastSeqNo)
        {
            string strSQL;
            SQLiteCommand sqlCmd;
            SQLiteDataReader productHistoryRd;

            chkMetaDBConn();

            try
            {
                strSQL = "SELECT PROGRESS FROM TB_PRODUCT_HISTORY WHERE SEQ = @seqno";
                sqlCmd = new SQLiteCommand(strSQL, metaDBConn);
                sqlCmd.Parameters.AddWithValue("@seqno", lastSeqNo);

                productHistoryRd = sqlCmd.ExecuteReader();
                sqlCmd.Dispose();

                if (productHistoryRd.Read())
                {
                    if ((long)(productHistoryRd["PROGRESS"]) == 80)
                    {
                        // remove completed file
                        if ((System.IO.File.Exists(sendfileFullPath)))
                        {
                            System.IO.File.Delete(sendfileFullPath);
                        }
                    }
                    else
                    {
                        log.Fatal("모든 과정 진행되었지만 progress가 80이 되지 않음. progress[" + productHistoryRd["PROGRESS"].ToString() + "], seqno[" + lastSeqNo.ToString() + "]");
                        productHistoryRd.Close();
                        return false;
                    }
                    productHistoryRd.Close();
                    return true;
                }
                else
                {
                    productHistoryRd.Close();
                    return false;
                }
            }
            catch (Exception e)
            {
                log.Error("seqno[" + lastSeqNo.ToString() + "] removeCompletedIdentity failed. messages : " + e.Message);
                return false;
            }
        }
        
        public Boolean MetaDBUpdate()
        {
            if ( chkMetaDBUpdate() )
            {
                if ( getDBData() )
                {
                    setMetaDBUpdateComplete();
                    return true;
                }
                else
                {
                    log.Error("metadata DB update fail");
                    return false;
                }
            }

            else
            {
                return false;
            }
        }

        private Boolean chkMetaDBUpdate()
        {
            JObject satelliteConfObj = JObject.Parse(File.ReadAllText(confFilePath));

            if ( satelliteConfObj["DB_DATA_UPDATE"].ToString()[0] == '1' )
            {
                return true;
            }

            else
            {
                return false;
            }
        }

        private void setMetaDBUpdateComplete()
        {
            JObject satelliteConfObj = JObject.Parse(File.ReadAllText(confFilePath));

            satelliteConfObj["DB_DATA_UPDATE"] = "0";

            File.WriteAllText(confFilePath, satelliteConfObj.ToString());
        }

    }



    // key : satellite명
    public class SatelliteInfo
    {
        private string[] reserve_time = new string[5];
        private int current_reserve_time_idx = -1;
        private int reserve_time_cnt = 5;

        private string next_reserve_time;
        public string NextReserveTime
        {
            get
            {
                return next_reserve_time;
            }
        } 

        private string launch_date;
        private string altitude;
        private string orbit;
        private string period;
        private string sensors;
        private string channel;
        private string resolution;
        private string swatch;
        private string sate_cd;
        private string application;

        // product informationi
        private List<ProductInfo> Product = new List<ProductInfo>();

        // Constructor
        public SatelliteInfo(SQLiteDataReader row)
        {
            string  reserveTimeColumnName;
            int     reserveTimeColumnIdx = 0;

            // 현재 시간 기준 다음 전송 시간 계산을 위한 변수
            DateTime reserveTime;
            DateTime currentTime;

            TimeSpan timeDiff;

            double minTimeInterval = 0.0;

            currentTime = DateTime.ParseExact(DateTime.Now.ToString("HHmmss"), "HHmmss", null);

            for ( int i = 0; i < 5 ; i++ )
            {
                reserveTimeColumnName = "RESERVE_TIME" + (++reserveTimeColumnIdx).ToString();
                reserve_time[i] = row[reserveTimeColumnName].ToString().Trim();

                // reserve_time이 존재하지 않을 경우는 바로 종료. reserve_time 기본 갯수는 5개
                if ( reserve_time[i].Length == 0 )
                {
                    reserve_time_cnt = i;
                    break;
                }

                reserveTime = DateTime.ParseExact(reserve_time[i], "HHmmss", null);                

                timeDiff = reserveTime - currentTime;

                if ( timeDiff.TotalSeconds > 0 )
                {
                    // 최소 시간 간격 최초 선택 시 또는 최소 시간 간격보다 작을 시
                    if ( current_reserve_time_idx == -1 || timeDiff.TotalSeconds < minTimeInterval )
                    {
                        minTimeInterval = timeDiff.TotalSeconds;
                        current_reserve_time_idx = i;
                        next_reserve_time = reserve_time[i];
                    }
                }
            }

            // 현재 시간이 RESERVE_TIME을 모두 지난 경우 첫번째 값으로 지정
            if ( current_reserve_time_idx == -1 )
            {
                current_reserve_time_idx = 0;
                next_reserve_time = reserve_time[0];
            }

            // next_reserve_time은 최종 선정 후 DB 업데이트 필요.


            launch_date = row["LAUNCH_DATE"].ToString().Trim();
            altitude = row["ALTITUDE"].ToString().Trim();
            orbit = row["ORBIT"].ToString().Trim();
            period = row["PERIOD"].ToString().Trim();
            sensors = row["SENSORS"].ToString().Trim();
            channel = row["CHANNEL"].ToString().Trim();
            resolution = row["RESOLUTION"].ToString().Trim();
            swatch = row["SWATCH"].ToString().Trim();
            sate_cd = row["SATE_CD"].ToString().Trim();
            application = row["APPLICATION"].ToString().Trim();
        }

        // set product data
        public void SetProduct(SQLiteDataReader row)
        {
            Product.Add(new ProductInfo(row));
        }

        // get product data
        public List<ProductInfo> GetProductInfo()
        {
            return Product;
        }

        public string SetNextReserveTime()
        {
            if ( ++current_reserve_time_idx >= reserve_time_cnt )
            {
                current_reserve_time_idx = 0;
            }

            next_reserve_time = reserve_time[current_reserve_time_idx];

            return next_reserve_time;
        }
    }

    public class ProductInfo
    {
        private string data_gbn;
        private string data_an_gbn;
        private string data_type;
        private string flienm_format;
        private string ftp_file_dir;

        public string DataGbn
        {
            get
            {
                return data_gbn;
            }
        }

        public string DataAnGbn
        {
            get
            {
                return data_an_gbn;
            }
        }

        public string DataType
        {
            get
            {
                return data_type;
            }
        }

        public string FlienmFormat
        {
            get
            {
                return flienm_format;
            }
        }
        public string FtpFileDir
        {
            get
            {
                return ftp_file_dir;
            }
        }


        // Constructor
        public ProductInfo(SQLiteDataReader row)
        {
            data_gbn = row["DATA_GBN"].ToString().Trim();
            data_an_gbn = row["DATA_AN_GBN"].ToString().Trim();
            data_type = row["DATA_TYPE"].ToString().Trim();
            flienm_format = row["FILENM_FORMAT"].ToString().Trim();
            ftp_file_dir = row["FTP_FILE_DIR"].ToString().Trim();
        }
    }

    public class IdentifierInfo
    {
        private string identifier;
        private string surveyDate;
        private string dataFormat;
        private string fileSize;
        private string outputFilePath;
        private string regDate;
        private string surveyTime;

        public string IdentifierNm
        {
            get
            {
                return identifier;
            }
        }
        public string SurveyDate
        {
            get
            {
                return surveyDate;
            }
        }

        public string DataFormat
        {
            get
            {
                return dataFormat;
            }
        }

        public string FileSize
        {
            get
            {
                return fileSize;
            }
        }

        public string OutputFilePath
        {
            get
            {
                return outputFilePath;
            }
        }

        public string RegDate
        {
            get
            {
                return regDate;
            }
        }

        public string SurveyTime
        {
            get
            {
                return surveyTime;
            }
        }

        // Constructor
        public IdentifierInfo(string fullFilePath, string dataType, string ftpFilePath)
        {
            FileInfo fileInfo = new FileInfo(fullFilePath);

            identifier = fileInfo.Name;
            surveyDate = getSurveyDate();
            dataFormat = identifier.Substring(identifier.LastIndexOf(".")+1, ( identifier.Length - ( identifier.LastIndexOf(".") + 1 )));
            fileSize = fileInfo.Length.ToString();

            outputFilePath = ftpFilePath;
            //outputFilePath = getOutputFilePath();
            regDate = DateTime.Now.ToString("yyyyMMdd");
            surveyTime = "";
        }

        public string getSurveyDate()
        {
            string resultSurveyDate = "";

            //"yyyymmdd.", "yyyymm.", "yyyy.mm.dd"
            MatchCollection matchCollection = Regex.Matches(identifier, "\\d{8}.|\\d{6}.|\\d{4}.\\d{2}.\\d{2}");
            
            // 첫번째로 일치하는 데이터만 보도록 하자
            if (matchCollection.Count > 0)
            {
                foreach (Match match in matchCollection)
                {
                    if ( match.ToString()[0] == '1' || match.ToString()[0] == '2' )
                    {

                        switch ( match.Length )
                        {
                            //yyyymmdd.
                            case 9:
                                resultSurveyDate = match.ToString().Substring(0, 8);
                                break;
                            //yyyymm.
                            case 7:
                                resultSurveyDate = match.ToString().Substring(0, 4) + match.ToString().Substring(5, 2) + "00";
                                break;
                            //yyyy.mm.dd
                            case 10:

                                resultSurveyDate = match.ToString().Substring(0, 4) + match.ToString().Substring(5, 2) + match.ToString().Substring(8, 2);
                                break;
                            default:
                                break;
                        }
                        break;
                    }
                    else
                    {
                        break;
                    }
                }
            }

            return resultSurveyDate;
        }

        /*
        public string getOutputFilePath()
        {

        }
        */

    }

    public class ServerInfo
    {
        private string server_name;
        private string conn_type;
        private string conn_addr;
        private string conn_portno;
        private string userid;
        private string password;
        private string db_sid;

        public string Conn_Addr
        {
            get
            {
                return conn_addr;
            }
        }

        public string Conn_portno
        {
            get
            {
                return conn_portno;
            }
        }

        public string Userid
        {
            get
            {
                return userid;
            }
        }

        public string Password
        {
            get
            {
                return password;
            }
        }

        public string DBSid
        {
            get
            {
                return db_sid;
            }
        }


        // Constructor
        public ServerInfo(SQLiteDataReader row)
        {
            server_name = row["SERVER_NAME"].ToString().Trim();
            conn_type = row["CONN_TYPE"].ToString().Trim();
            conn_addr = row["CONN_ADDR"].ToString().Trim();
            conn_portno = row["CONN_PORTNO"].ToString().Trim();
            userid = row["USERID"].ToString().Trim();
            password = row["PASSWORD"].ToString().Trim();
            db_sid = row["DB_SID"].ToString().Trim();
        }
    }

}
