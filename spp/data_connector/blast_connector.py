# -*- coding: utf-8 -*-
"""
Created on Fri July 01 20:20:53 2019

"""

# Just another comment

__author__ = "Geotechnical Monitoring Team/Munkhtsolmon Munkhchuluun"

#######################################################
# Creating Blast flat-file from CaveCad and Pitram ####
#######################################################

import pandas as pd
import pymssql
import os
import numpy as np
from datetime import datetime, timedelta
from dateutil.tz import tzoffset
import glob
import sqlite3 as sql3

from microquake.core.settings import settings

sql3.register_adapter(np.int64, int)
sql3.register_adapter(np.float64, float)

# Setting up working directory (where Blast_Log.db must have located)
os.chdir('//tuul/Data/UndergroundOperations/10.0 Geosciences/Geotechnical/'
         '27 Monitoring/20_Personnal_Folders/Munkhtsolmon.M/scripts/blast''')


# Connecting to Pitram
def connect_Pitram(Days_Old=30):
    host = 'mnoytsql15'
    database = 'PITRAMReporting'
    user = r'Psreportreader'
    password = "KFR7T4LWwk"  # use of permanent account is recommended.
    conn = pymssql.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    # following "sql" query was made by OTUG MMC/Uliisaikhan Olonbayar
    # the query will be filtered by days old
    sql = '''Select
    *
    ,isnull(MineCycle1.Chainage,(max(MineCycle1.Chainage) over (partition by MineCycle1.[CycleLocation] order by MineCycle1.[CycleLocation] asc, MineCycle1.[CycleStartDate] asc))) as MaxChainage
    ,isnull(MineCycle1.Chainage,(min(MineCycle1.Chainage) over (partition by MineCycle1.[CycleLocation] order by MineCycle1.[CycleLocation] asc, MineCycle1.[CycleStartDate] asc))) as MinChainage

    from
    (
    Select
    *
    ,(case when FullRoundCount=0 and ((StripCount>0 and StripRound=0 ) or SafetyBayCount>0) then 'Strip'
           When [LastCycle]<>'Last Cycle' and  FullRoundCount=0 and StripCount=0 and SafetyBayCount=0 and Lead([CycleLocation]) over (partition by [CycleLocation] order by [CycleLocation] asc, [CycleStartDate] asc)=[CycleLocation] then 'Not completed'
    else 'Full' end ) CycleStatus

    from
    (Select top 100 percent
    [MineCycleID]
    ,[CycleLocation]
    ,max([CycleStartDate]) [CycleStartDate]
    ,max([CycleEndDate]) [CycleEndDate]
    ,cast(sum(coalesce(ChargeDur,0))/3600 as decimal(30,1)) ChargeDur
    ,sum(coalesce(NumberHoles,0))  NumberHoles
    ,sum(coalesce(Watergel,0))  Watergel
    ,sum(coalesce(FullRound,0)) FullRound
    ,count(FullRound) FullRoundCount
    ,sum(coalesce(StripRound,0)) StripRound
    ,count(StripRound) StripCount
    ,sum(coalesce(SafetyBay,0)) SafetyBay
    ,count(SafetyBay) SafetyBayCount
    ,[LastCycle]
    ,max(Chainage) Chainage
    ,MIN([EventDateTime]) BlastedTime
    from 
    (
    Select 
    [MineCycleID]
    ,[MineCycleID2]
    ,[CycleLocation]
    ,[CycleStartDate]
    ,[CycleEndDate]
    ,[LastCycle]
    ,(Case when [Equipment Function Description] in ('Drill','Boltbore') and ([Allocation Status]='101 - Drilling' or [Allocation Status]='117 - Operating Low') and [Measure Description]='Chainage From' then [Measure Value] 
            Else null end) as Chainage
    ,(Case when [Equipment Function Description]='Charge Up' and [Allocation Status]='102 - Charging Explosive' and [Measure Description]='Seconds' then [Measure Value] 
            Else null end) as ChargeDur
    ,(Case when [Equipment Function Description]='Charge Up' and [Allocation Status]='102 - Charging Explosive' 
                                                              and [Measure Description] in ('Face holes 2.0m charged quantity',
                                                                                              'Face holes 3.0m charged quantity',
                                                                                              'Face holes 4.0m charged quantity',
                                                                                              'Face holes 4.7m charged quantity',
                                                                                              'Face holes 5.2m charged quantity',
                                                                                              'Face holes 5.7m charged quantity',
                                                                                              'Face Holes 6.2m charged Quantity',
                                                                                              'Strip holes charged quantity')
                                                              then [Measure Value] 
            Else null end) as NumberHoles
    ,(Case when [Equipment Function Description]='Charge Up' and [Allocation Status]='102 - Charging Explosive' 
                                                              and [Measure Description] in ('Watergel')
                                                              then [Measure Value]
            Else null end) as Watergel
    ,(Case when [Equipment Function Description]='Blast' and ([Allocation Status]='101 - Drilling') and [Measure Description] in ('Round Advance [eqm]') then [Measure Value] 
            Else null end) as FullRound
    ,(Case when [Equipment Function Description]='Blast' and ([Allocation Status]='101 - Drilling') and [Measure Description] in ('Strip Advance [eqm]') then [Measure Value] 
            Else null end) as StripRound
    ,(Case when [Equipment Function Description]='Blast' and ([Allocation Status]='101 - Drilling') and [Measure Description] in ('Safety Bay Advance [eqm]') then [Measure Value] 
            Else null end) as SafetyBay
    ,(Case when [Equipment Function Description]='Blast' and ([Allocation Status]='101 - Drilling') then [EventDateTime]
            Else null end) as [EventDateTime]

    from (
    Select
    *
    from
    (
    SELECT top 100 percent
            T.[MineCycleID]
            ,T.[Source_DevPrd]
            ,T.[MineCycleID2]
            ,T.Status_Description
            ,T.Equipment_Description
            ,T.[CycleLocation]
            ,T.[CycleStartDate]
            --Mine Cycle date will be set as next round start
            ,(CASE WHEN  Lead(T.[CycleLocation]) over (order by T.[CycleLocation] asc, T.[CycleStartDate] asc) <> T.[CycleLocation] THEN GETDATE()
                     ELSE  Lead(T.[CycleStartDate]) over (order by T.[CycleLocation] asc, T.[CycleStartDate] asc)
                     END) as [CycleEndDate]
            ,(CASE WHEN  Lead(T.[CycleLocation]) over (order by T.[CycleLocation] asc, T.[CycleStartDate] asc) <> T.[CycleLocation] THEN 'Last Cycle'
                     ELSE 'Past'
                     END) as [LastCycle]
    FROM
        ( select top 100 percent
       --Mine Cycle ID set as the date(time) when drilling started--
            (case
                when right(ALLOCTNTIMESTAMP.[ShKey],2)='P1' then left(ALLOCTNTIMESTAMP.[ShKey],8)+'DS'
                when right(ALLOCTNTIMESTAMP.[ShKey],2)='P2' then left(ALLOCTNTIMESTAMP.[ShKey],8)+'NS'
            end) AS 'MineCycleID'
           ,ALLOCTNTIMESTAMP.[ShKey]+' '+V_LOCATIONS.Source_Description as [MineCycleID2]
           ,ALLOCTNTIMESTAMP.[EventDateTime] AS 'CycleStartDate'
           ,V_STATUS.Status_Description
           ,V_EQUIPMENT.Equipment_Description
           ,(CASE WHEN ALLOCTNTIMESTAMP.[EventDateTime]<dateadd (hour , 15 , LAG(ALLOCTNTIMESTAMP.[EventDateTime]) over (order by V_LOCATIONS.Source_Description asc, ALLOCTNTIMESTAMP.[EventDateTime] asc)) AND V_LOCATIONS.Source_Description=LAG(V_LOCATIONS.Source_Description) over (order by V_LOCATIONS.Source_Description asc, ALLOCTNTIMESTAMP.[EventDateTime] asc)
                    THEN 'FROM PREVIOUS SHIFT'
                    ELSE 'YES'
                    END
                )[MINECYCLESTATUS]
           --To eliminate duplicated measure entered in same timestamp--
           ,row_number() over(partition by ALLOCTNTIMESTAMP.[EventDateTime] order by ALLOCTNTIMESTAMP.eventdatetime asc) AS RWNMB
           --Cycle End date. If cycle did not finish date will get current date and time--
           ,V_LOCATIONS.Source_Description AS 'CycleLocation'
           ,[Source_DevPrd]
    FROM ALLOCTNTIMESTAMP
    with (NOLOCK)
            INNER JOIN MEASURETIMESTAMP
                ON ALLOCTNTIMESTAMP.TSKey = MEASURETIMESTAMP.TSKey
            INNER JOIN V_LOCATIONS
                ON ALLOCTNTIMESTAMP.Location = V_LOCATIONS.SourceCode
            INNER JOIN V_MEASCODE
                ON V_MEASCODE.Measure_Code = MEASURETIMESTAMP.MeasCode
            INNER JOIN V_STATUS
                ON ALLOCTNTIMESTAMP.Status = V_STATUS.STATUSCODE
            INNER JOIN V_EQUIPMENT
                ON V_EQUIPMENT.EquipmentCode = ALLOCTNTIMESTAMP.Equipment
            INNER JOIN V_EQUIPMENT_MODEL
                ON V_EQUIPMENT.Equipment_Model_Code = V_EQUIPMENT_MODEL.Equipment_Model_Code
            INNER JOIN V_EQUIPMENT_FUNCTION
                ON V_EQUIPMENT_MODEL.Equipment_Model_Function_Code = V_EQUIPMENT_FUNCTION.Equipment_Function_Code
           --Mine Cylce Begins when Drilling primary status entered--
           --PD001 has Drilling status but for uphole drilling
           --UDD001 has drilling status but for exploring
            where V_STATUS.Status_Description in ('101 - Drilling') and V_EQUIPMENT_FUNCTION.Equipment_Function_Description in ('Drill', 'Bolter', 'Boltbore') and V_EQUIPMENT.Equipment_Description not in ('PD001','UDD001') --and  V_MEASCODE.Measure_Description in  ('Face Holes 2.0m Drilled Quantity', 'Face Holes 2.4m Drilled Quantity', 'Face Holes 3.0m Drilled Quantity','Face Holes 4.0m Drilled Quantity','Face holes 4.7m drilled quantity','Face Holes 5.2m Drilled Quantity','Face holes 5.7m drilled quantity','Face Holes 6.2m Drilled Quantity')
            and ALLOCTNTIMESTAMP.[EventDateTime]> dateadd(day,{},getdate())
            Order by V_LOCATIONS.Source_Description asc, ALLOCTNTIMESTAMP.[EventDateTime] asc
        ) as T
    Where T.RWNMB=1 AND [MINECYCLESTATUS]<>'FROM PREVIOUS SHIFT'
    order by T.[CycleLocation]  asc ,T.[CycleStartDate] asc) as [Cycle]
    Left Join (SELECT
           (case 
                when right(ALLOCTNTIMESTAMP.[ShKey],2)='P1' then left(ALLOCTNTIMESTAMP.[ShKey],8)+'DS'
                when right(ALLOCTNTIMESTAMP.[ShKey],2)='P2' then left(ALLOCTNTIMESTAMP.[ShKey],8)+'NS'
           end) AS 'Mine Shift'
           ,ALLOCTNTIMESTAMP.[EventDateTime]
           ,V_MEASCODE.Measure_Description AS 'Measure Description'
           ,MEASURETIMESTAMP.MeasureValue AS 'Measure Value'
           ,V_LOCATIONS.Source_Description AS 'Location'
           ,V_EQUIPMENT.Equipment_Description AS 'Equipment'
           ,V_EQUIPMENT_FUNCTION.Equipment_Function_Description AS 'Equipment Function Description'
           ,V_OPERATORS.Operator_Description AS 'Operator Name'
           ,V_STATUS.Status_Description AS 'Allocation Status'
    FROM
            ALLOCTNTIMESTAMP
            with (nolock)
            INNER JOIN MEASURETIMESTAMP
            ON ALLOCTNTIMESTAMP.TSKey = MEASURETIMESTAMP.TSKey
            INNER JOIN V_LOCATIONS
            ON ALLOCTNTIMESTAMP.Location = V_LOCATIONS.SourceCode
            INNER JOIN V_EQUIPMENT
            ON V_EQUIPMENT.EquipmentCode = ALLOCTNTIMESTAMP.Equipment
            INNER JOIN V_EQUIPMENT_MODEL
            ON V_EQUIPMENT.Equipment_Model_Code = V_EQUIPMENT_MODEL.Equipment_Model_Code
            INNER JOIN V_EQUIPMENT_FUNCTION
            ON V_EQUIPMENT_MODEL.Equipment_Model_Function_Code = V_EQUIPMENT_FUNCTION.Equipment_Function_Code
            INNER JOIN V_MEASCODE
            ON V_MEASCODE.Measure_Code = MEASURETIMESTAMP.MeasCode
            INNER JOIN V_STATUS
            ON ALLOCTNTIMESTAMP.Status = V_STATUS.STATUSCODE
            INNER JOIN V_OPERATORS
            ON ALLOCTNTIMESTAMP.Operator = V_OPERATORS.OperatorCode
            where V_EQUIPMENT_FUNCTION.Equipment_Function_Description in ('Drill', 'Boltbore','Bolter', 'Charge up','Loader', 'Shotcrete')
            and ALLOCTNTIMESTAMP.[EventDateTime]>dateadd(day,{},getdate())
            and Status_Description in ('101 - Drilling',
                                        '102 - Charging Explosive',
                                        '104 - Mucking',
                                        '106 - Shotcreting 1st Pass',
                                        '108 - Rockbolting or Meshing 1st Pass',
                                        '117 - Operating Low')
            and Measure_Description in ('Face Holes 2.0m Drilled Quantity',
                                        'Face Holes 2.4m Drilled Quantity',
                                        'Face Holes 3.0m Drilled Quantity',
                                        'Face Holes 4.0m Drilled Quantity',
                                        'Face holes 4.7m drilled quantity',
                                        'Face Holes 5.2m Drilled Quantity',
                                        'Face holes 5.7m drilled quantity',
                                        'Face Holes 6.2m Drilled Quantity',
                                        'Strip holes drilled quantity',
                                        'Face holes 2.0m charged quantity',
                                        'Face holes 3.0m charged quantity',
                                        'Face holes 4.0m charged quantity',
                                        'Face holes 4.7m charged quantity',
                                        'Face holes 5.2m charged quantity',
                                        'Face holes 5.7m charged quantity',
                                        'Face Holes 6.2m charged Quantity',
                                        'Strip holes charged quantity',
                                        'Chainage From',
                                        'Watergel')

    UNION all

    SELECT
           (case 
                when right([LOCATIONMEASURES].[SHKEY],2)='P1' then left([LOCATIONMEASURES].[SHKEY],8)+'DS'
                when right([LOCATIONMEASURES].[SHKEY],2)='P2' then left([LOCATIONMEASURES].[SHKEY],8)+'NS'
           end)AS 'Mine Shift'
          ,[EventDateTime]
          ,V_MEASCODE.Measure_Description
          ,[MeasureValue]
          ,V_LOCATIONS.Source_Description
          ,(CASE when V_MEASCODE.Measure_Description<>'' then 'Blast' else ' ' end ) AS LocationEquip
          ,(CASE when V_MEASCODE.Measure_Description<>'' then 'Blast' else ' ' end ) AS LocationEquipFunc
          ,(CASE when V_MEASCODE.Measure_Description<>'' then null else ' ' end ) AS LocationOper
          ,(CASE when V_MEASCODE.Measure_Description<>'' then '101 - Drilling' else ' ' end ) AS LocationAllocSta

      FROM [PITRAMReporting].[dbo].[LOCATIONMEASURES]
      with (nolock)
        INNER JOIN V_LOCATIONS
        ON [LOCATIONMEASURES].[Location]= V_LOCATIONS.SourceCode
        INNER JOIN V_MEASCODE
        ON V_MEASCODE.Measure_Code = [LOCATIONMEASURES].[MeasureCode]
        WHERE V_MEASCODE.Measure_Description in ('Round Advance [eqm]','Safety Bay Advance [eqm]','Strip Advance [eqm]')
        and [EventDateTime]>dateadd(day,{},getdate())) as [Master] on [Master].Location=[Cycle].[CycleLocation] and Cycle.[CycleStartDate]<=[Master].[EventDateTime] and Cycle.[CycleEndDate]>[Master].[EventDateTime]
        ) as CycleCom ) as Cyclefinal

        group by [MineCycleID] ,[MineCycleID2] ,[CycleLocation],[LastCycle]
    ) MineCycle
    ) MineCycle1'''.format(-Days_Old, -Days_Old, -Days_Old)
    df_Pitram = pd.read_sql(sql, conn)
    '''Data pre-processing
    1. Formatting Location of Data in order to match Pitram with CaveCad
    1.1 "Location_modified" indicates the location name without
    multi-attribute. The first round of matches takes the chainage and
    location into account. So, change in multi-atrribute will not
    affect accuracy of match.'''
    blastlocation = []
    for date in df_Pitram['CycleLocation']:
        try:
            if (len((date.replace("-", "")).replace("_", "")) > 8) \
                    & ("_" in (date.split('-')[2])):
                blastlocation.append((date.replace("-", ""))
                                     .replace("_", "")[0:4] + "-" +
                                     (date.replace("-", ""))
                                     .replace("_", "")[4:8] +
                                     "-" + "____")
            elif (len((date.replace("-", "")).replace("_", "")) > 8) \
                    & ("_" not in (date.split('-')[2])):
                blastlocation.append((date.replace("-", ""))
                                     .replace("_", "")[0:4] + "-" +
                                     (date.replace("-", ""))
                                     .replace("_", "")[4:8] + "-" +
                                     date.split('-')[2])
            elif (len((date.replace("-", "")).replace("_", "")) <= 8):
                blastlocation.append((date.replace("-", ""))
                                     .replace("_", "")[0:4] + "-" +
                                     (date.replace("-", ""))
                                     .replace("_", "")[4:8] + "-" +
                                     date.split('-')[2])
        except:
            blastlocation.append((date.replace("-", "")).replace("_", "")[0:4]
                                 + "-" + (date.replace("-", ""))
                                 .replace("_", "")[4:8])

    df_Pitram['Location_modified'] = blastlocation
    '''1.2 'Location_modified_exact' refers actual location name. "-", "_", and
    "____" are reformatted'''
    blastlocation = []
    for date in df_Pitram['CycleLocation']:
        try:
            if (len((date.replace("-", "")).replace("_", "")) > 8) \
                    & ("_" in (date.split('-')[2])):
                blastlocation.append((date.replace("-", ""))
                                     .replace("_", "")[0:4] + "-" +
                                     (date.replace("-", ""))
                                     .replace("_", "")[4:8] + "-" + "____"
                                     + "-" + date.split('-')[3])
            elif (len((date.replace("-", "")).replace("_", "")) > 8) \
                    & ("_" not in (date.split('-')[2])):
                blastlocation.append((date.replace("-", ""))
                                     .replace("_", "")[0:4] + "-" +
                                     (date.replace("-", ""))
                                     .replace("_", "")[4:8] + "-" +
                                     date.split('-')[2] + "-" +
                                     date.split('-')[3])
            elif (len((date.replace("-", "")).replace("_", "")) <= 8):
                blastlocation.append((date.replace("-", ""))
                                     .replace("_", "")[0:4] + "-" +
                                     (date.replace("-", ""))
                                     .replace("_", "")[4:8] + "-" +
                                     date.split('-')[2])
        except:
            blastlocation.append((date.replace("-", "")).replace("_", "")[0:4]
                                 + "-" + (date.replace("-", ""))
                                 .replace("_", "")[4:8])
    df_Pitram['Location_modified_exact'] = blastlocation

    # 2. Removing Pitram logged blasts which does not have timestamp.
    endtime = datetime.now().replace(tzinfo=tzoffset('uln', 8 * 3600)). \
        __format__("%m/%d/%y %H:%M:%S")
    df_Pitram = df_Pitram[((df_Pitram['BlastedTime'] <= endtime))]. \
        reset_index(drop=True)
    df_Pitram['UniqueID'] = list(map(lambda date, location: (str(date) + '/'
                                                             + location),
                                     df_Pitram['BlastedTime'],
                                     df_Pitram['CycleLocation']))
    return df_Pitram


df_Pitram = connect_Pitram(Days_Old=60)

'''Get_unmatched_Data and approximate match from last iteration to the
new iteration in order to reduce amount of data to match'''


def append_unmatched_appriximate():
    input_dir = (r'\\tuul\Data\UndergroundOperations\10.0 Geosciences'
                 r'\Geotechnical\27 Monitoring\20_Personnal_Folders'
                 r'\Munkhtsolmon.M\scripts\blast')
    tmp = glob.glob(input_dir + r'\Blast_Lo*')
    conn = sql3.connect(tmp[0])
    sql = '''Select
    * FROM Matched_Blasts
    WHERE Correctness = 'Accurate'
    '''
    df_Blast_Log_Matched = pd.read_sql(sql, conn)
    df_Pitram.index = df_Pitram['UniqueID']
    df_Pitram.drop(list(pd.DataFrame(list(map(lambda x: x
    if (x in df_Pitram['UniqueID'])
    else np.nan,
                                              (df_Blast_Log_Matched['UniqueID']
                                              )))).dropna()[0]), inplace=True
                   )
    sql = '''Select
    * FROM Unmatched_Blasts
    '''

    df_Blast_Log_Unmatched = pd.read_sql(sql, conn)

    df_Pitram_append = df_Pitram.append(df_Blast_Log_Unmatched, sort=False) \
        .drop_duplicates(subset=['UniqueID'])
    conn.close()
    df_Pitram_append.reset_index(drop=True, inplace=True)
    # Giving each blast an ID

    Index = []
    for i in range(0, len(df_Pitram_append)):
        Index.append(i)
    df_Pitram_append['Index'] = Index
    return df_Pitram_append


df_Pitram = append_unmatched_appriximate()


# Connectig to CaveCad


def connect_cavecad(Days_Old=30):
    host = 'mnoytsql42'
    database = 'CaveCadOT'
    user = 'cavecad'
    password = 'C4v3C4d.12345'
    conn = pymssql.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )

    sql = '''prc_ExcavationAttributes'''

    df = pd.read_sql(sql, conn)[['LOMDriveName',
                                 'ChainageFrom', 'ChainageTo', 'FromX',
                                 'FromY',
                                 'FromZ', 'ToX', 'ToY', 'ToZ', 'Date Fired',
                                 'ExcavationDate']]
    conn.close()
    endtime = datetime.now().replace(tzinfo=tzoffset('uln', 8 * 3600))
    starttime = endtime - timedelta(days=Days_Old)
    df = df[(df['ExcavationDate'] > pd.Timestamp(starttime)
             .__format__("%Y-%m-%d"))].reset_index(drop=True)

    '''Data pre-processing
    1. Formatting Location of Data in order to match CaveCad with Pitram
    1.1 "Location_modified" indicates the location name without
    multi-attribute.The first round of the matches takes the chainage and
    location into account. So, change in multi-atrribute (which does not
    make the chainage to alter) in the drive will not affect accuracy of match.
    '''

    blastlocation = []
    for date in df['LOMDriveName']:
        try:
            if (len((date.split('N1-')[1].replace("-", ""))
                            .replace("_", "")) > 8) & \
                    ("_" in (date.split('-')[3])):
                blastlocation.append(((str(date.split('N1-')[1]))
                                      .replace("-", "")).replace("_", "")[0:4]
                                     + "-" + ((str(date.split('N1-')[1]))
                                              .replace("-", "")).replace("_",
                                                                         "")[
                                             4:8]
                                     + "-" + "____")
            elif (len((date.split('N1-')[1].replace("-", ""))
                              .replace("_", "")) > 8) \
                    & ("_" not in (date.split('-')[3])):
                blastlocation.append(((str(date.split('N1-')[1]))
                                      .replace("-", "")).replace("_", "")[0:4]
                                     + "-" + ((str(date.split('N1-')[1]))
                                              .replace("-", "")).replace("_",
                                                                         "")[
                                             4:8]
                                     + "-" + str(date).split('-')[3])
            elif (len((date.split('N1-')[1].replace("-", ""))
                              .replace("_", "")) == 8):
                blastlocation.append((((str(date.split('N1-')[1]))
                                       .replace("-", "")).replace("_", ""))[
                                     0:4]
                                     + "-" + str(date.split('N1-')[1])
                                     .replace("-", "").replace("_", "")[4:8])
            elif (len((date.split('N1-')[1].replace("-", ""))
                              .replace("_", "")) < 8):
                blastlocation.append((str(date.split('N1-')[1]))
                                     .replace("-", "").replace("_", "")[0:4])
        except:
            blastlocation.append(date)

    df['Location_modified'] = blastlocation

    blastlocation = []

    for date in df['LOMDriveName']:
        try:
            if (len(date.split('N1-')[1].replace("-", "")
                            .replace("_", "")) > 8) & \
                    ("_" in (date.split('-')[3])):
                blastlocation.append(((str(date.split('N1-')[1]))
                                      .replace("-", "")).replace("_", "")[0:4]
                                     + "-" + ((str(date.split('N1-')[1]))
                                              .replace("-", "")).replace("_",
                                                                         "")[
                                             4:8]
                                     + "-" + "____" + "-"
                                     + str(date).split('-')[4])
            elif (len((date.split('N1-')[1].replace("-", ""))
                              .replace("_", "")) > 8) \
                    & ("_" not in (date.split('-')[3])):
                blastlocation.append(((str(date.split('N1-')[1]))
                                      .replace("-", "")).replace("_", "")[0:4]
                                     + "-" + ((str(date.split('N1-')[1]))
                                              .replace("-", "")).replace("_",
                                                                         "")[
                                             4:8]
                                     + "-" + str(date).split('-')[3]
                                     + "-" + str(date).split('-')[4])
            elif (len((date.split('N1-')[1].replace("-", ""))
                              .replace("_", "")) == 8):
                blastlocation.append((((str(date.split('N1-')[1]))
                                       .replace("-", "")).replace("_", ""))[
                                     0:4]
                                     + "-" + (str(date.split('N1-')[1])
                                              .replace("-", "")).replace("_",
                                                                         "")[
                                             4:8])
            elif (len((date.split('N1-')[1].replace("-", ""))
                              .replace("_", "")) < 8):
                blastlocation.append((str(date.split('N1-')[1]))
                                     .replace("-", "").replace("_", "")[0:4])
        except:
            blastlocation.append(date)

    df['Location_modified_exact'] = blastlocation

    '''2. bounding the excavation attributes in CaveCad by mine area.
    there were few clearly wrong coordinates in CaveCad'''
    df = df[((df['ToX'] < 654506) & (df['ToX'] > 645828) &
             (df['ToY'] < 4770037) & (df['ToY'] > 4764445))] \
        .reset_index(drop=True)
    return df


df = connect_cavecad(Days_Old=60)


# Iteration #1 - Merging based on chainage and location

def Merge_on_chainage_exact():
    data = {}
    data['CycleLocation'] = []
    data['ChainageFrom'] = []
    data['ChainageTo'] = []
    data['X'] = []
    data['Y'] = []
    data['Z'] = []
    data['Watergel'] = []
    data['BlastedTime'] = []
    data["Index"] = []
    data["Correctness"] = []
    data["Min_Error"] = []
    Index1 = []
    # Location must match. Chainage must be within the Pitram chainage range.
    for i in range(0, len(df)):
        for l in range(0, len(df_Pitram)):
            if (df_Pitram['Location_modified'][l] == df['Location_modified'][
                i]) \
                    and (df_Pitram['MaxChainage'][l] >= df['ChainageFrom'][i]) \
                    and (df_Pitram['MaxChainage'][l] < df['ChainageTo'][i]):
                data['CycleLocation'].append(df_Pitram['CycleLocation'][l])
                data['ChainageFrom'].append(df['ChainageFrom'][i])
                data['ChainageTo'].append(df['ChainageTo'][i])
                data['X'].append(df['ToX'][i])
                data['Y'].append(df['ToY'][i])
                data['Z'].append(df['ToZ'][i])
                data['Watergel'].append(df_Pitram['Watergel'][l])
                data['BlastedTime'].append(df_Pitram['BlastedTime'][l])
                data["Index"].append(l)
                data["Correctness"].append('Accurate')
                Index1.append(df_Pitram['Index'][l])
                data["Min_Error"].append(abs(df_Pitram['MaxChainage'][l]
                                             - df['ChainageTo'][i]))

    df_matched_accurate = pd.DataFrame(data).drop_duplicates()
    df_matched_accurate = df_matched_accurate.loc[df_matched_accurate.groupby
    (['Index'], sort=False)
    ['Min_Error'].idxmin()]
    df_matched_accurate.reset_index(drop=True, inplace=True)
    return df_matched_accurate, Index1


df_matched_accurate = Merge_on_chainage_exact()[0]
Index1 = Merge_on_chainage_exact()[1]

df_unmatched = df_Pitram.drop(list(set(Index1)))
df_unmatched.reset_index(drop=True, inplace=True)

'''Iteration #2 - Merging based on DataFrame "df_matched_accurate" and
left unmatched blast logs. If there are a unmatched blasts which blasted
within 2 weeks, treat the "df_matched_accurate" coordinate as approximate for
unmatched blasts - Name must match, blasted date must be within 2 weeks'''


def Merge_on_matched_date_approximate(days_range=14):
    data = {}
    data['CycleLocation'] = []
    data['ChainageFrom'] = []
    data['ChainageTo'] = []
    data['X'] = []
    data['Y'] = []
    data['Z'] = []
    data['Watergel'] = []
    data['BlastedTime'] = []
    data["Correctness"] = []
    data["Index"] = []
    data["Min_Error"] = []
    Index2 = []

    for i in range(0, len(df_unmatched)):
        for l in range(0, len(df_matched_accurate)):
            if (df_matched_accurate['CycleLocation'][l]
                == df_unmatched['CycleLocation'][i]) \
                    & (pd.Timestamp(df_unmatched['BlastedTime'][i]) -
                       pd.Timestamp(df_matched_accurate["BlastedTime"][l])
                       < timedelta(days=+days_range)) \
                    & (pd.Timestamp(df_unmatched['BlastedTime'][i]) -
                       pd.Timestamp(df_matched_accurate["BlastedTime"][l])
                       > timedelta(days=0)):
                data['CycleLocation'].append(
                    df_matched_accurate['CycleLocation'][l])
                data['ChainageFrom'].append(
                    df_matched_accurate['ChainageFrom'][l])
                data['ChainageTo'].append(df_matched_accurate['ChainageTo'][l])
                data['X'].append(df_matched_accurate['X'][l])
                data['Y'].append(df_matched_accurate['Y'][l])
                data['Z'].append(df_matched_accurate['Z'][l])
                data['Watergel'].append(df_unmatched['Watergel'][i])
                data['BlastedTime'].append(df_unmatched['BlastedTime'][i])
                data["Correctness"].append('Approximate')
                data["Index"].append(i)
                Index2.append(df_unmatched['Index'][i])
                data["Min_Error"].append(df_unmatched['BlastedTime'][i] -
                                         df_matched_accurate["BlastedTime"][l])

    df_matched_approx_00 = pd.DataFrame(data).drop_duplicates()
    df_matched_approx_00 = df_matched_approx_00.loc[df_matched_approx_00.
        groupby(['Index'],
                sort=False)
    ['Min_Error'].idxmin()]
    return df_matched_approx_00, Index2


df_matched_approx_00 = Merge_on_matched_date_approximate(days_range=14)[0]
Index2 = Merge_on_matched_date_approximate(days_range=14)[1]

df_unmatched = df_Pitram.drop(list(set(Index2 + Index1)))
df_unmatched.reset_index(drop=True, inplace=True)

'''Iteration #3 - Finding coordinate of blast based on predictive mode.

Accurate 2 coordinates of blasts in same drive will make a line.
As the majority of the drives are straight, it is safe to assume that
given chainage of the unknown blasts can be used in predicting coordinates (as
a definition of point in 3D line) - see func().If the unknown blasts's chainage
is within within 30m range of known blasts chainage, it is considered here.
Once the Geotechnical Data Management team process the blast, this approximate
coordinate will be replaced by accurate blast coordinate.

'''


def func(x1, y1, z1, x2, y2, z2, m1, m2, m3):
    dist = np.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2 + (z1 - z2) ** 2)
    V = np.array([(x1 - x2) / dist, (y1 - y2) / dist, (z1 - z2) / dist])
    if (m1 >= m3) and (m2 >= m3) and (m1 > m2):
        d = m2 - m3
        Prod = np.array([x2, y2, z2]) - d * V
    elif (m1 >= m3) and (m2 >= m3) and (m1 < m2):
        d = m1 - m3
        Prod = np.array([x1, y1, z1]) - d * V
    elif (m1 <= m3) and (m2 >= m3):
        d = m3 - m1
        Prod = np.array([x1, y1, z1]) + d * V
    elif (m1 >= m3) and (m2 <= m3):
        d = m3 - m2
        Prod = np.array([x2, y2, z2]) + d * V
    elif (m1 <= m3) and (m2 <= m3) and (m1 < m2):
        d = m2 - m3
        Prod = np.array([x2, y2, z2]) + d * V
    elif (m1 <= m3) and (m2 <= m3) and (m1 > m2):
        d = m1 - m3
        Prod = np.array([x1, y1, z1]) + d * V
    else:
        Prod = np.full(3, np.nan)
    return Prod


def Merge_on_predictive_mode_approximate(Chainage_range=30):
    data = {}
    data['CycleLocation'] = []
    data['ChainageFrom'] = []
    data['ChainageTo'] = []
    data['ToX'] = []
    data['ToY'] = []
    data['ToZ'] = []
    data['FromX'] = []
    data['FromY'] = []
    data['FromZ'] = []
    data['Watergel'] = []
    data['BlastedTime'] = []
    data["Correctness"] = []
    data["Index"] = []
    data["Min_Error"] = []

    for i in range(0, len(df)):
        for l in range(0, len(df_unmatched)):
            if (df_unmatched['Location_modified'][l]
                == df['Location_modified'][i]) and \
                    (abs(df_unmatched['MaxChainage'][l] - df['ChainageTo'][i])
                     < Chainage_range):
                data['CycleLocation'].append(df_unmatched['CycleLocation'][l])
                data['ChainageFrom'].append(df['ChainageFrom'][i])
                data['ChainageTo'].append(df['ChainageTo'][i])
                data['ToX'].append(df['ToX'][i])
                data['ToY'].append(df['ToY'][i])
                data['ToZ'].append(df['ToZ'][i])
                data['FromX'].append(df['FromX'][i])
                data['FromY'].append(df['FromY'][i])
                data['FromZ'].append(df['FromZ'][i])
                data['Watergel'].append(df_unmatched['Watergel'][l])
                data['BlastedTime'].append(df_unmatched['BlastedTime'][l])
                data["Correctness"].append('Approximate')
                data["Index"].append(df_unmatched['Index'][l])
                data["Min_Error"].append(abs(df_unmatched['MaxChainage'][l]
                                             - df['ChainageTo'][i]))

    df_missed01 = pd.DataFrame(data)
    # Picking closest 2 blasts in the drive from the unknown blasts
    df_missed02 = df_missed01.sort_values('Min_Error'). \
        groupby("Index", sort=False, as_index=False).nth(0)
    df_missed03 = df_missed01.sort_values('Min_Error'). \
        groupby("Index", sort=False, as_index=False).nth(1)
    df_missed04 = df_missed02.append(df_missed03, ignore_index=True)

    data = {}
    data['CycleLocation'] = []
    data['ChainageFrom'] = []
    data['ChainageTo'] = []
    data['X'] = []
    data['Y'] = []
    data['Z'] = []
    data['Watergel'] = []
    data['BlastedTime'] = []
    data["Correctness"] = []
    data["Index"] = []
    data["Min_Error"] = []
    Index3 = []

    for i in range(0, len(df_missed04)):
        for l in range(0, len(df_unmatched)):
            if (len(df_missed04[df_missed04.Index
                                == df_missed04["Index"][i]]) == 2) \
                    and (df_unmatched["Index"][l] == df_missed04["Index"][i]) \
                    and (
                    (df_missed04[df_missed04.Index == df_missed04["Index"][i]]
                            .reset_index()['ChainageTo'][0]) != (
                    df_missed04[df_missed04.Index
                                == df_missed04["Index"][i]].reset_index()[
                        'ChainageTo'][1])) \
                    and (df_unmatched['MaxChainage'][l] >= 0):
                function = func(df_missed04[df_missed04.Index ==
                                            df_missed04["Index"][
                                                i]].reset_index()['ToX'][0],
                                df_missed04[df_missed04.Index ==
                                            df_missed04["Index"][
                                                i]].reset_index()['ToY'][0],
                                df_missed04[df_missed04.Index ==
                                            df_missed04["Index"][
                                                i]].reset_index()['ToZ'][0],
                                df_missed04[df_missed04.Index ==
                                            df_missed04["Index"][
                                                i]].reset_index()['ToX'][1],
                                df_missed04[df_missed04.Index ==
                                            df_missed04["Index"][
                                                i]].reset_index()['ToY'][1],
                                df_missed04[df_missed04.Index ==
                                            df_missed04["Index"][
                                                i]].reset_index()['ToZ'][1],
                                df_missed04[df_missed04.Index ==
                                            df_missed04["Index"][
                                                i]].reset_index()[
                                    'ChainageTo'][0],
                                df_missed04[df_missed04.Index ==
                                            df_missed04["Index"][
                                                i]].reset_index()[
                                    'ChainageTo'][1],
                                df_unmatched["MaxChainage"][l])
                data['X'].append(function[0])
                data['Y'].append(function[1])
                data['Z'].append(function[2])
                data['CycleLocation'].append(df_unmatched['CycleLocation'][l])
                data['ChainageFrom'].append(np.nan)
                data['ChainageTo'].append(np.nan)
                data['Watergel'].append(df_unmatched['Watergel'][l])
                data['BlastedTime'].append(df_unmatched['BlastedTime'][l])
                data["Correctness"].append('Approximate')
                data["Index"].append(df_unmatched["Index"][l])
                Index3.append(df_unmatched['Index'][l])
                data["Min_Error"].append("0")
            elif (len(df_missed04[
                          df_missed04.Index == df_missed04["Index"][i]]) == 1) \
                    and (df_unmatched["Index"][l] == df_missed04["Index"][i]) \
                    and (df_unmatched['MaxChainage'][l] >= 0):
                function = func(df_missed04[df_missed04.Index ==
                                            df_missed04["Index"][
                                                i]].reset_index()['ToX'][0],
                                df_missed04[df_missed04.Index ==
                                            df_missed04["Index"][
                                                i]].reset_index()['ToY'][0],
                                df_missed04[df_missed04.Index ==
                                            df_missed04["Index"][
                                                i]].reset_index()['ToZ'][0],
                                df_missed04[df_missed04.Index ==
                                            df_missed04["Index"][
                                                i]].reset_index()['FromX'][0],
                                df_missed04[df_missed04.Index ==
                                            df_missed04["Index"][
                                                i]].reset_index()['FromY'][0],
                                df_missed04[df_missed04.Index ==
                                            df_missed04["Index"][
                                                i]].reset_index()['FromZ'][0],
                                df_missed04[df_missed04.Index ==
                                            df_missed04["Index"][
                                                i]].reset_index()[
                                    'ChainageTo'][0],
                                df_missed04[df_missed04.Index ==
                                            df_missed04["Index"][
                                                i]].reset_index()[
                                    'ChainageFrom'][0],
                                df_unmatched["MaxChainage"][l])
                data['X'].append(function[0])
                data['Y'].append(function[1])
                data['Z'].append(function[2])
                data['CycleLocation'].append(df_unmatched['CycleLocation'][l])
                data['ChainageFrom'].append(np.nan)
                data['ChainageTo'].append(np.nan)
                data['Watergel'].append(df_unmatched['Watergel'][l])
                data['BlastedTime'].append(df_unmatched['BlastedTime'][l])
                data["Correctness"].append('Approximate')
                data["Index"].append(df_unmatched["Index"][l])
                Index3.append(df_unmatched['Index'][l])
                data["Min_Error"].append("0")

    df_matched_approx_01 = pd.DataFrame(data).drop_duplicates()
    df_matched_approx_01 = df_matched_approx_01.groupby("Index", sort=False,
                                                        as_index=False).nth(0)
    return df_matched_approx_01, Index3


df_matched_approx_01 = Merge_on_predictive_mode_approximate(Chainage_range=30)[
    0]
Index3 = Merge_on_predictive_mode_approximate(Chainage_range=30)[1]

df_unmatched = df_Pitram.drop(list(set(Index3 + Index1 + Index2)))
df_unmatched.reset_index(drop=True, inplace=True)

'''Iteration #4 - Merging based on approximately matching blasted times in
Pitram and CaveCad. The day, Data Management team finished processing scanning
was logged in CaveCad as "ExcavationDate". Even though there are "Date Fired"
attribute, it has inconsistent formatting and accuracy was shown to be
questionable. Therefore, in this iteration, Pitram blasted time and
location name will be matched with CaveCad data processed date and
location name with 14 days range.
in CaveCad and matching heading name
(ExcavationDate)
'''


def merge_on_cavecad_processed_date_approximate(days_range=10):
    df['ExcavationDate'] = pd.to_datetime(df['ExcavationDate'])
    data = {}
    data['CycleLocation'] = []
    data['ChainageFrom'] = []
    data['ChainageTo'] = []
    data['X'] = []
    data['Y'] = []
    data['Z'] = []
    data['Watergel'] = []
    data['BlastedTime'] = []
    data["Correctness"] = []
    data["Index"] = []
    Index4 = []
    data["Min_Error"] = []

    for i in range(0, len(df_unmatched)):
        for l in range(0, len(df)):
            if (df['Location_modified_exact'][l] ==
                df_unmatched['Location_modified_exact'][i]) \
                    & ((pd.Timestamp(
                df_unmatched['BlastedTime'][i]) - pd.Timestamp(
                df["ExcavationDate"][l]))
                       < timedelta(days=+days_range)) \
                    & ((pd.Timestamp(
                df_unmatched['BlastedTime'][i]) - pd.Timestamp(
                df["ExcavationDate"][l]))
                       > timedelta(days=0)):
                data['CycleLocation'].append(df_unmatched['CycleLocation'][i])
                data['ChainageFrom'].append(df['ChainageFrom'][l])
                data['ChainageTo'].append(df['ChainageTo'][l])
                data['X'].append(df['ToX'][l])
                data['Y'].append(df['ToY'][l])
                data['Z'].append(df['ToZ'][l])
                data['Watergel'].append(df_unmatched['Watergel'][i])
                data['BlastedTime'].append(df_unmatched['BlastedTime'][i])
                data["Correctness"].append('Approximate')
                data["Index"].append(i)
                Index4.append(df_unmatched['Index'][i])
                data["Min_Error"].append(
                    df_unmatched['BlastedTime'][i] - df["ExcavationDate"][l])

    df_matched_approx_02 = pd.DataFrame(data).drop_duplicates()
    df_matched_approx_02 = df_matched_approx_02.loc[df_matched_approx_02.
        groupby(['Index'],
                sort=False)
    ['Min_Error'].idxmin()]
    return df_matched_approx_02, Index4


df_matched_approx_02 = \
merge_on_cavecad_processed_date_approximate(days_range=10)[0]
Index4 = merge_on_cavecad_processed_date_approximate(days_range=10)[1]

df_unmatched = df_Pitram.drop(list(set(Index4 + Index3 + Index2 + Index1)))
df_unmatched.reset_index(drop=True, inplace=True)

df_unmatched['UniqueID'] = list(map(lambda date, location: (str(date) + '/' +
                                                            location),
                                    df_unmatched['BlastedTime'],
                                    df_unmatched['CycleLocation']))

df_matched_all = df_matched_accurate.append(
    [df_matched_approx_00, df_matched_approx_01,
     df_matched_approx_02], sort=False)
df_matched_all.reset_index(drop=True, inplace=True)

df_matched_all['UniqueID'] = list(
    map(lambda date, location: (str(date) + '/' + location) \
        , df_matched_all['BlastedTime'], df_matched_all['CycleLocation']))


def write_to_sql():
    conn = sql3.connect('Blast_Log.db')
    c = conn.cursor()

    for i in range(0, len(df_matched_all)):
        c.execute('REPLACE into Matched_Blasts values (?,?,?,?,?,?,?,?,?,?)',
                  [str(df_matched_all['BlastedTime'][i]),
                   df_matched_all['ChainageFrom'][i],
                   df_matched_all['ChainageTo'][i],
                   df_matched_all['Correctness'][i],
                   df_matched_all['CycleLocation'][i],
                   df_matched_all['Watergel'][i],
                   df_matched_all['X'][i],
                   df_matched_all['Y'][i], df_matched_all['Z'][i],
                   df_matched_all['UniqueID'][i]])

    for i in range(0, len(df_unmatched)):
        c.execute(
            'REPLACE into Unmatched_Blasts values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            [df_unmatched['MineCycleID'][i],
             df_unmatched['CycleLocation'][i],
             str(df_unmatched['CycleStartDate'][i]),
             str(df_unmatched['CycleEndDate'][i]),
             df_unmatched['ChargeDur'][i],
             df_unmatched['NumberHoles'][i],
             df_unmatched['Watergel'][i],
             df_unmatched['FullRound'][i],
             df_unmatched['FullRoundCount'][i],
             df_unmatched['StripRound'][i],
             df_unmatched['StripCount'][i],
             df_unmatched['SafetyBay'][i],
             df_unmatched['SafetyBayCount'][i],
             df_unmatched['LastCycle'][i],
             df_unmatched['Chainage'][i],
             str(df_unmatched['BlastedTime'][i]),
             df_unmatched['CycleStatus'][i],
             df_unmatched['MaxChainage'][i],
             df_unmatched['MinChainage'][i],
             df_unmatched['Location_modified'][i],
             df_unmatched['Location_modified_exact'][i],
             df_unmatched['Index'][i],
             df_unmatched['UniqueID'][i]])
    conn.commit()
    conn.close()


write_to_sql()
