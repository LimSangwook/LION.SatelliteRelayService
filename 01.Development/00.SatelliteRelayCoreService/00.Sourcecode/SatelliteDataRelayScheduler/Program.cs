using System;
using System.Collections.Generic;
using System.Data.SQLite;
using log4net;
using log4net.Config;
using System.Xml;
using System.Reflection;
using Newtonsoft.Json.Linq;
using System.IO;

namespace SatelliteDataRelayScheduler
{
    class Program
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(Program));
        private static string satelliteConfPath = @"conf\satelliteScheduler_config.json";
        private static string log4netConfPath = @"conf\SatelliteLog4net.config";


        static void Main(string[] args)
        {
            // read config file
            JObject satelliteConfObj = JObject.Parse(File.ReadAllText(satelliteConfPath));

            setLog4netConf(satelliteConfObj["LOG_PATH"].ToString(), satelliteConfObj["LOG_LEVEL"].ToString());

            var repo = log4net.LogManager.CreateRepository(Assembly.GetEntryAssembly(), typeof(log4net.Repository.Hierarchy.Hierarchy));
            
            XmlConfigurator.ConfigureAndWatch(repo, new System.IO.FileInfo("conf/SatelliteLog4net.config"));

            SatelliteData scheduleData = new SatelliteData(satelliteConfObj);

            SatelliteScheduler scheduler = new SatelliteScheduler(scheduleData, satelliteConfObj);
           
            if ( scheduler.OnScheduling() )
            {

            }
        }

        private static void setLog4netConf(string logPath, string logLevel)
        {
            XmlDocument log4netConfig = new XmlDocument();            
            XmlNodeList log4netNode;            
            XmlAttributeCollection log4netNodeAttr;

            log4netConfig.Load(log4netConfPath);

            // set logpath
            log4netNode = log4netConfig.SelectNodes("/descendant::log4net/appender/param");

            for (int i = 0; i < log4netNode.Count; i++)
            {
                log4netNodeAttr = log4netNode[i].Attributes;
                if (log4netNodeAttr.GetNamedItem("name").Value == "File")
                {
                    log4netNodeAttr.GetNamedItem("value").Value = logPath;
                    break;
                }
            }

            // set loglevel
            log4netNode = log4netConfig.SelectNodes("/descendant::log4net/root/level");

            for (int i = 0; i < log4netNode.Count; i++)
            {
                log4netNodeAttr = log4netNode[i].Attributes;
                log4netNodeAttr.GetNamedItem("value").Value = logLevel;                    
            }

            log4netConfig.Save(log4netConfPath);
        }
    }
}
