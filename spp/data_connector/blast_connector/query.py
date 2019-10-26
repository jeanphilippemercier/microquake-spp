def sql(days_old):
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
    ) MineCycle1'''.format(-days_old, -days_old, -days_old)
    return sql
