using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json.Linq;
using log4net;
using log4net.Config;
using System.Reflection;

namespace SatelliteDataRelayScheduler
{
    //TODO
    // 1) 정규식으로 파일 가져오기(스케쥴링 시간 안되도 미리 가져와서 db에 업데이트 할 것인가...)
    //    -> 파일 전송 완료여부는 현재시간과 파일 write 한 시간의 시간차이로 보기... 
    // 2) 스케쥴링 : 깊은 대기, 얕은 대기
    // (3) ~ 4)는 파일별 thread로 진행..)
    // 3) FTP 파일 전송
    // 4) 고객 원격 DB 업데이트
    class SatelliteScheduler
    {
        private SatelliteData ScheduleData;

        private int LONG_SLEEP;
        private int SHORT_SLEEP;

        // 가장 인접한 시간과의 간격이 SHORT_TIME_TRIGGER_DIFF 이하로 들어오면 SHORT_SLEEP으로 변경.
        // 반대로 가장 인접한 시간과의 간격이 SHORT_TIME_TRIGGER_DIFF 이상으로 늘어나면 LONG_SLEEP으로 재차 변경.
        private double SHORT_TIME_TRIGGER_DIFF;

        private int sleepTime;

        private static readonly ILog log = LogManager.GetLogger(typeof(Program));

        // Constructor
        public SatelliteScheduler( SatelliteData initScheduleData, JObject satelliteConfObj)
        {
            ScheduleData = initScheduleData;

            // set configuration data
            LONG_SLEEP = satelliteConfObj["SCHEDULING_TIME"]["LONG_SLEEP"].ToObject<int>();
            SHORT_SLEEP = satelliteConfObj["SCHEDULING_TIME"]["SHORT_SLEEP"].ToObject<int>();
            SHORT_TIME_TRIGGER_DIFF = satelliteConfObj["SCHEDULING_TIME"]["SHORT_TIME_TRIGGER_DIFF"].ToObject<double>();

            var repo = log4net.LogManager.CreateRepository(Assembly.GetEntryAssembly(), typeof(log4net.Repository.Hierarchy.Hierarchy));

            XmlConfigurator.ConfigureAndWatch(repo, new System.IO.FileInfo("conf/SatelliteLog4net.config"));
        }

        // scheduling main method
        public Boolean OnScheduling()
        {
            List<string> TargetSatellite = new List<string>();
            SetSleepTime(ScheduleData.GetMinTimeInterval());

            int prevSleepTime = sleepTime;

            while (true)
            {
                System.Threading.Thread.Sleep(sleepTime * 1000);

                // 1) 수행대상 satellite 확인
                TargetSatellite = ScheduleData.GetTargetSatellite();

                // 2) 대상 satellite 작업 수행
                //    - 대상 파일 확인
                //    - FTP 파일 전송
                //    - DB INSERT
                //    - DELETE FILE
                for ( int i = 0; i < TargetSatellite.Count ; i++ )
                {
                    Console.WriteLine("name:"+TargetSatellite[i]+",reserve_time:"+ ScheduleData.GetSatelliteInfo(TargetSatellite[i]).NextReserveTime);
                    ScheduleData.RelaySatellite(TargetSatellite[i]);
                }

                if ( TargetSatellite.Count > 0 )
                {
                    // 3) 발동 후 대상 satellite list 초기화
                    TargetSatellite.Clear();                    
                }

                SetSleepTime(ScheduleData.GetMinTimeInterval());

                if (prevSleepTime != sleepTime)
                {
                    prevSleepTime = sleepTime;
                    log.Info("sleep time change : " + sleepTime);
                }

                // checking metadata database update when long sleep time
                if ( sleepTime == LONG_SLEEP )
                {
                    if ( ScheduleData.MetaDBUpdate() )
                    {
                        log.Info("metadata DB update success");
                    }
                }
            }
            return false;
        }

        // set sleep time
        public void SetSleepTime(double minTimeInterval)
        {
            // NEXT_RESERVE_TIME이 현재 시간 이후인 시간이 없거나, trigger 시간보다 많이 남은 경우
            if ( minTimeInterval < 0 || minTimeInterval > SHORT_TIME_TRIGGER_DIFF )
            {
                sleepTime = LONG_SLEEP;
            }
            else
            {
                sleepTime = SHORT_SLEEP;
            }
        }
    }
}
