# [ODS_Pitram_DOMEProduction], \
# [ODS_Pitram_Pitram_Custom], \
# [ODS_Pitram_PITRAMReporting]

# [Pitram_DOMEProduction],
# [Pitram_Pitram_Custom],
# [Pitram_PITRAMReporting]

# these databases already created in mnoytsqlc2
# [DOMEProduction],
# [Pitram_Custom],
# [PITRAMReporting]

#trialing less loop
import os
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from dateutil.tz import tzoffset
from query import sql
from sqlalchemy import create_engine, MetaData, Table, Column, \
    DateTime, Float, CHAR, TEXT, INTEGER
from dynaconf import settings
from microquake.helpers.logging import logger
from pyodbc import OperationalError

__author__ = "Geotechnical Monitoring Team/Munkhtsolmon Munkhchuluun"


class BlastConnector(object):
    """This class will create blast_log (X, Y, X, DateTime, Accuracy, Heading name).
    """
    def __init__(self, module_name="blast_connector"):
        """
        :param module_name: name of the module, the name must be coherent
        with a section in the config file.
        :param days_old: defining date filter [aka. call last "days_old"]
        :param sql: sql script, filtered by "days_old", to read connect_pitram
        :param input_dir: location of the blast_log sqlite db - having the
        DB means not to regenerate matched blast logs.
        :param days_range: defining - for the same heading, how many days
        old blasts are to be considered proper approximation in coordinate
        of unprocessed blast (laserscanning)
        :param chainage_range: defining - for the blast with chainage and
        without coordinates, known closer (chainage_range) blast with
        chainages and with coordinate will be used to calculate coordinate
        of unknown coordinate using definition of point in 3D line
        :return: None
        """
        self.settings = settings
        self.module_name = module_name
        self.days_old = self.settings.get(module_name)['days_old']
        self.sql = sql(self.days_old)
        self.input_dir = self.settings.get(module_name)['input_dir']
        self.days_range = self.settings.get(module_name)['days_range']
        self.chainage_range = self.settings.get(module_name)['chainage_range']
        self.start_time = datetime.now().replace(tzinfo=tzoffset('uln',
                                                                8*3600))

    def connect_pitram(self):
        """Returns a pd.DataFrame with blast related attributes, sourced
        from pitram DB. It filtered by days_old. The DataFrame was
        pre-processed to conform with latter matching algorithms.

        :param module_name.connect_pitram: parameters to connect to Pitram
        MSSQL DB.
        :raises [ErrorType]: [ErrorDescription]
        :return: a pd.DataFrame filtered by days_old in the config file
        :rtype: pd.DataFrame
        """
        self.host = self.settings.get(self.module_name)[
            'connect_pitram']['host']
        self.database = self.settings.get(self.module_name)[
            'connect_pitram']['database']
        self.user = self.settings.get(self.module_name)[
            'connect_pitram']['user']
        self.password = self.settings.get(self.module_name)[
            'connect_pitram']['password']
        self.port = self.settings.get(self.module_name)[
            'connect_pitram']['port']
        engine = create_engine("mssql+pyodbc://" +
                               self.user + ":" + self.password + "@" +
                               self.host + ":" + self.port + "/" +
                               self.database +
                               "?driver=ODBC+Driver+17+for+SQL+Server")
        conn = engine.connect()
        df_pitram = pd.read_sql(self.sql, conn)

        '''Data pre-processing
        1. Formatting location of blasts in order to match Pitram with 
        CaveCad  
        1.1 "Location_modified" indicates the location name without
        multi-attribute. The first round of matches takes the chainage and
        location into account. So, change in multi-atrribute will not
        affect accuracy of match.'''
        blastlocation = []
        for date in df_pitram['CycleLocation']:
            counter = 0
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
            except Exception as e:
                counter += 1
                print(str(e) + " Pitram: " +
                      str(counter) + " location(s) name was/were not "
                                     "recorded under right convention")
                blastlocation.append((date.replace("-", "")).replace(
                    "_", "")[0:4] + "-" + (date.replace("-", ""))
                                     .replace("_", "")[4:8])

        df_pitram['Location_modified'] = blastlocation
        '''1.2 'Location_modified_exact' refers actual location name. 
        "-", "_", and  "____" are reformatted'''
        blastlocation = []
        for date in df_pitram['CycleLocation']:
            counter = 0
            try:
                if (len((date.replace("-", "")).replace("_", "")) > 8) \
                        & ("_" in (date.split('-')[2])):
                    blastlocation.append((date.replace("-", ""))
                                         .replace("_", "")[0:4] + "-" +
                                         (date.replace("-", ""))
                                         .replace("_", "")[4:8] + "-" +
                                         "____" + "-" + date.split('-')[3])
                elif (len((date.replace("-", "")).replace("_", "")) > 8) \
                        & ("_" not in (date.split('-')[2])):
                    blastlocation.append((date.replace("-", ""))
                                         .replace("_", "")[0:4] + "-" +
                                         (date.replace("-", ""))
                                         .replace("_", "")[4:8] + "-" +
                                         date.split('-')[2] + "-" +
                                         date.split('-')[3])
                elif len((date.replace("-", "")).replace("_", "")) <= 8:
                    blastlocation.append((date.replace("-", ""))
                                         .replace("_", "")[0:4] + "-" +
                                         (date.replace("-", ""))
                                         .replace("_", "")[4:8] + "-" +
                                         date.split('-')[2])
            except Exception as e:
                counter += 1
                print(str(e) + " Pitram: " +
                      str(counter) + " location(s) name was/were not "
                                     "recorded under right convention")
                blastlocation.append((date.replace("-", "")).replace(
                    "_", "")[0:4] + "-" + (date.replace("-", ""))
                                     .replace("_", "")[4:8])
        df_pitram['Location_modified_exact'] = blastlocation

        # 2. Removing Pitram logged blasts which does not have timestamp.
        endtime = datetime.now().replace(tzinfo=
                                         tzoffset('uln',
                                                  8 * 3600))._format__(
            "%m/%d/%y %H:%M:%S")
        df_pitram = df_pitram[df_pitram['BlastedTime'] <= endtime]. \
            reset_index(drop=True)
        df_pitram['UniqueID'] = list(map(lambda date, location: (str(
            date) + '/' + location), df_pitram['BlastedTime'],
                                         df_pitram['CycleLocation']))
        return df_pitram

    def connect_cavecad(self):
        """Returns a pd.DataFrame with blast related attributes, sourced
        from cavecad DB. it is filtered by days_old. The DataFrame was
        pre-processed to conform with latter matching algorithms.
        :param module_name.connect_pitram: parameters to connect to Pitram
        MSSQL DB.
        :raises [ErrorType]: [ErrorDescription]
        :return: a pd.DataFrame filtered by days_old in the config file
        :rtype: pd.DataFrame
        """
        self.host = self.settings.get(self.module_name)[
            'connect_cavecad']['host']
        self.database = self.settings.get(self.module_name)[
            'connect_cavecad']['database']
        self.user = self.settings.get(self.module_name)[
            'connect_cavecad']['user']
        self.password = self.settings.get(self.module_name)[
            'connect_cavecad']['password']
        self.port = self.settings.get(self.module_name)[
            'connect_cavecad']['port']

        engine = create_engine("mssql+pyodbc://" +
                               self.user + ":" + self.password + "@" +
                               self.host + ":" + self.port + "/" +
                               self.database +
                               "?driver=ODBC+Driver+17+for+SQL+Server")
        conn = engine.connect()
        sql = '''prc_ExcavationAttributes'''

        df = pd.read_sql(sql, conn)[['LOMDriveName',
                                     'ChainageFrom', 'ChainageTo',
                                     'FromX', 'FromY', 'FromZ', 'ToX',
                                     'ToY', 'ToZ', 'Date Fired',
                                     'ExcavationDate']]
        conn.begin().commit()
        conn.close()
        engine.dispose()
        endtime = datetime.now().replace(tzinfo=tzoffset('uln', 8 * 3600))
        starttime = endtime - timedelta(days=self.days_old)
        df = df[(df['ExcavationDate'] > pd.Timestamp(starttime)
                 .__format__("%Y-%m-%d"))].reset_index(drop=True)

        '''Data pre-processing
        1. Formatting Location of Data in order to match CaveCad with 
        Pitram 
        1.1 "Location_modified" indicates the location name without
        multi-attribute.The first round of the matches takes the 
        chainage and location into account. So, change in 
        multi-atrribute (which does not make the chainage to alter) in 
        the drive will not affect accuracy of match. 
        '''

        blastlocation = []
        for date in df['LOMDriveName']:
            counter = 0
            try:
                if (len((date.split('N1-')[1].replace("-", ""))
                                .replace("_", "")) > 8) & \
                        ("_" in (date.split('-')[3])):
                    blastlocation.append(((str(date.split('N1-')[1]))
                                          .replace("-", "")).replace("_", "")[
                                         0:4] +
                                         "-" + ((str(date.split('N1-')[1]))
                                                .replace("-", "")).replace("_",
                                                                           "")[
                                               4:8] +
                                         "-" + "____")
                elif (len((date.split('N1-')[1].replace("-", ""))
                                  .replace("_", "")) > 8) \
                        & ("_" not in (date.split('-')[3])):
                    blastlocation.append(((str(date.split('N1-')[1]))
                                          .replace("-", "")).replace("_", "")[
                                         0:4] +
                                         "-" + ((str(date.split('N1-')[1]))
                                                .replace("-", "")).replace("_",
                                                                           "")[
                                               4:8] +
                                         "-" + str(date).split('-')[3])
                elif (len((date.split('N1-')[1].replace("-", ""))
                                  .replace("_", "")) == 8):
                    blastlocation.append((((str(date.split('N1-')[1]))
                                           .replace("-", "")).replace("_",
                                                                      ""))[
                                         0:4] +
                                         "-" + str(date.split('N1-')[1])
                                         .replace("-", "").replace("_", "")[
                                               4:8])
                elif (len((date.split('N1-')[1].replace("-", ""))
                                  .replace("_", "")) < 8):
                    blastlocation.append((str(date.split('N1-')[1]))
                                         .replace("-", "").replace("_", "")[
                                         0:4])
            except Exception as e:
                counter += 1
                print(str(e) + " CaveCad: " +
                      str(counter) + " location(s) name was/were not "
                                     "recorded under right convention")
                blastlocation.append(date)

        df['Location_modified'] = blastlocation

        blastlocation = []

        for date in df['LOMDriveName']:
            counter = 0
            try:
                if (len(date.split('N1-')[1].replace("-", "")
                                .replace("_", "")) > 8) & \
                        ("_" in (date.split('-')[3])):
                    blastlocation.append(((str(date.split('N1-')[1]))
                                          .replace("-", "")).replace("_", "")[
                                         0:4] +
                                         "-" + ((str(date.split('N1-')[1]))
                                                .replace("-", "")).replace("_",
                                                                           "")[
                                               4:8] +
                                         "-" + "____" + "-" +
                                         str(date).split('-')[4])
                elif (len((date.split('N1-')[1].replace("-", ""))
                                  .replace("_", "")) > 8) \
                        & ("_" not in (date.split('-')[3])):
                    blastlocation.append(((str(date.split('N1-')[1]))
                                          .replace("-", "")).replace("_", "")[
                                         0:4] +
                                         "-" + ((str(date.split('N1-')[1]))
                                                .replace("-", "")).replace("_",
                                                                           "")[
                                               4:8] +
                                         "-" + str(date).split('-')[3] +
                                         "-" + str(date).split('-')[4])
                elif (len((date.split('N1-')[1].replace("-", ""))
                                  .replace("_", "")) == 8):
                    blastlocation.append((((str(date.split('N1-')[1]))
                                           .replace("-", "")).replace("_",
                                                                      ""))[
                                         0:4] +
                                         "-" + (str(date.split('N1-')[1])
                                                .replace("-", "")).replace("_",
                                                                           "")[
                                               4:8])
                elif (len((date.split('N1-')[1].replace("-", ""))
                                  .replace("_", "")) < 8):
                    blastlocation.append((str(date.split('N1-')[1]))
                                         .replace("-", "").replace("_", "")[
                                         0:4])
            except Exception as e:
                counter += 1
                print(str(e) + " CaveCad: " +
                      str(counter) + " location(s) name was/were not "
                                     "recorded under right convention")
                blastlocation.append(date)

        df['Location_modified_exact'] = blastlocation

        '''2. bounding the excavation attributes in CaveCad by mine area.
        there were few clearly wrong coordinates in CaveCad'''
        df = df[((df['ToX'] < 654506) & (df['ToX'] > 645828) &
                 (df['ToY'] < 4770037) & (df['ToY'] > 4764445))] \
            .reset_index(drop=True)
        return df

    def append_unmatched_approximate(self, df_pitram):
        """Calling matched blast_log from last iterations [sqlite db]
        and exclude those from this iteration [df_pitram] using primary
        key. Contrarily, fetching unmatched data from last iteration
        [sqlite db] and append it to newly generated blasts [df_pitram].
        if the sqlite db does not exist, create an empty one.

        :raises [ErrorType]: [ErrorDescription]
        :return: modified connect_pitram [df_pitram] pd.DataFrame
        :rtype: pd.DataFrame
        """
        # df_pitram = self.df_pitram
        try:
            engine = create_engine("sqlite:///" + self.input_dir,
                                   echo=False)
            conn = engine.connect()
            sql = '''Select
            * FROM Matched_Blasts
            WHERE Correctness = 'Accurate'
            '''
            df_blast_log_matched = pd.read_sql(sql, conn)
            df_pitram.index = df_pitram['UniqueID']
            df_pitram.drop(list(pd.DataFrame(list(map(lambda x: x
                                                      if (x in df_pitram['UniqueID'])
                                                      else np.nan,
                                                      (df_blast_log_matched['UniqueID']
                                                       )))).dropna()[0]), inplace=True
                           )
            sql = '''Select
            * FROM Unmatched_Blasts
            '''

            df_blast_log_unmatched = pd.read_sql(sql, conn)

            df_pitram = df_pitram.append(df_blast_log_unmatched, sort=False) \
                .drop_duplicates(subset=['UniqueID'])
            df_pitram.reset_index(drop=True, inplace=True)
            # Giving each blast an ID
            Index = []
            for i in range(0, len(df_pitram)):
                Index.append(i)
            df_pitram['Index'] = Index
            conn.begin().commit()
            conn.close()
            engine.dispose()
            return df_pitram
        except Exception as e:
            print(e)
            conn.begin().commit()
            conn.close()
            engine.dispose()
            engine = create_engine("sqlite:///" + self.input_dir,
                                   echo=False)
            conn = engine.connect()
            meta = MetaData()
            Matched_Blasts = Table(
                'Matched_Blasts', meta,
                Column('BlastedTime', DateTime),
                Column('ChainageFrom', Float),
                Column('ChainageTo', Float),
                Column('Correctness', TEXT),
                Column('CycleLocation', CHAR),
                Column('Watergel', Float),
                Column('Easting', Float),
                Column('Northing', Float),
                Column('Elevation', Float),
                Column('UniqueID', CHAR, primary_key=True, unique=True),
            )

            Unmatched_Blasts = Table(
                'Unmatched_Blasts', meta,
                Column('MineCycleID', CHAR),
                Column('CycleLocation', CHAR),
                Column('CycleStartDate', DateTime),
                Column('CycleEndDate', DateTime),
                Column('ChargeDur', Float),
                Column('NumberHoles', Float),
                Column('Watergel', Float),
                Column('FullRound', Float),
                Column('FullRoundCount', Float),
                Column('StripRound', CHAR),
                Column('StripCount', Float),
                Column('SafetyBay', Float),
                Column('SafetyBayCount', Float),
                Column('LastCycle', CHAR),
                Column('Chainage', Float),
                Column('BlastedTime', DateTime),
                Column('CycleStatus', CHAR),
                Column('MaxChainage', Float),
                Column('MinChainage', Float),
                Column('Location_modified', CHAR),
                Column('Location_modified_exact', CHAR),
                Column('Index', INTEGER),
                Column('UniqueID', CHAR, primary_key=True, unique=True),
            )
            meta.create_all(engine)
            conn.begin().commit()
            conn.close()
            engine.dispose()
            # Giving each blast an ID
            df_pitram.reset_index(drop=True, inplace=True)
            Index = []
            for i in range(0, len(df_pitram)):
                Index.append(i)
            df_pitram['Index'] = Index
            return df_pitram

    def merge_on_chainage_exact(self, df_pitram, df):
        """Iteration #1 - Merging based on matching chainage and
        location in both connect_cavecad (coordinate) and connect_pitram
        (blast date and time)
        :raises [ErrorType]: [ErrorDescription]
        :return: tuple (df_matched_accurate - accurately matched set of
        blasts, index1 - index of them in initial connect_pitram
        [df_pitram] to exclude them in next iterations, df_unmatched -
        ]remaining unmatched blast_logs in connect_pitram [pitram])
        :rtype: tuple(pd.DataFrame, list, pd.DataFrame)
        """
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
        index1 = []
        # Location must match. Chainage must be within the Pitram
        # chainage range.
        for i in range(0, len(df)):
            for l in range(0, len(df_pitram)):
                if (df_pitram['Location_modified'][l] ==
                    df['Location_modified'][i]) \
                        and (
                        df_pitram['MaxChainage'][l] >= df['ChainageFrom'][i]) \
                        and (
                        df_pitram['MaxChainage'][l] < df['ChainageTo'][i]):
                    data['CycleLocation'].append(df_pitram['CycleLocation'][l])
                    data['ChainageFrom'].append(df['ChainageFrom'][i])
                    data['ChainageTo'].append(df['ChainageTo'][i])
                    data['X'].append(df['ToX'][i])
                    data['Y'].append(df['ToY'][i])
                    data['Z'].append(df['ToZ'][i])
                    data['Watergel'].append(df_pitram['Watergel'][l])
                    data['BlastedTime'].append(df_pitram['BlastedTime'][l])
                    data["Index"].append(l)
                    data["Correctness"].append('Accurate')
                    index1.append(df_pitram['Index'][l])
                    data["Min_Error"].append(abs(df_pitram['MaxChainage'][l] -
                                                 df['ChainageTo'][i]))

        df_matched_accurate = pd.DataFrame(data).drop_duplicates()
        df_matched_accurate = df_matched_accurate.loc[
            df_matched_accurate.groupby
            (['Index'], sort=False)
            ['Min_Error'].idxmin()]
        df_matched_accurate.reset_index(drop=True, inplace=True)
        df_unmatched = df_pitram.drop(list(set(index1)))
        df_unmatched.reset_index(drop=True, inplace=True)

        return df_matched_accurate, index1, df_unmatched

    def merge_on_matched_date_approximate(self, df_unmatched,
                                          df_matched_accurate, df_pitram,
                                          index1):
        """Iteration #2 - approximate matching on "df_matched_accurate" -
        connect_pitram with coordinate and left unmatched blast logs -
        connect_pitram without coordinate.

        Unmatched blasts which blasted within 2 weeks of
        "df_matched_accurate" blasts; then, adopt their coordinate
        as approximate - Heading name must match, blasted date must be
        within days_range [scenario: 5 heading in 2 weeks = 25m].
        Once the Geotechnical Data Management team process the blast,
        this approximate coordinate can be replaced by accurate blast
        coordinate in next iteration.

        :raises [ErrorType]: [ErrorDescription]
        :return: tuple (df_matched_approx_00 - approximately matched set of
        blasts, index2 - index of them in initial connect_pitram
        [df_pitram] to exclude them in next iterations, df_unmatched -
        remaining unmatched blast_logs in connect_pitram [pitram])
        :rtype: tuple(pd.DataFrame, list, pd.DataFrame)
        """
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
        index2 = []
        for i in range(0, len(df_unmatched)):
            for l in range(0, len(df_matched_accurate)):
                if (df_matched_accurate['CycleLocation'][l] ==
                    df_unmatched['CycleLocation'][i]) \
                        & (pd.Timestamp(df_unmatched['BlastedTime'][i]) -
                           pd.Timestamp(
                               df_matched_accurate["BlastedTime"][l]) <
                           timedelta(days=+self.days_range)) \
                        & (pd.Timestamp(df_unmatched['BlastedTime'][i]) -
                           pd.Timestamp(
                               df_matched_accurate["BlastedTime"][l]) >
                           timedelta(days=0)):
                    data['CycleLocation'].append(
                        df_matched_accurate['CycleLocation'][l])
                    data['ChainageFrom'].append(
                        df_matched_accurate['ChainageFrom'][l])
                    data['ChainageTo'].append(
                        df_matched_accurate['ChainageTo'][l])
                    data['X'].append(df_matched_accurate['X'][l])
                    data['Y'].append(df_matched_accurate['Y'][l])
                    data['Z'].append(df_matched_accurate['Z'][l])
                    data['Watergel'].append(df_unmatched['Watergel'][i])
                    data['BlastedTime'].append(df_unmatched['BlastedTime'][i])
                    data["Correctness"].append('Approximate')
                    data["Index"].append(i)
                    index2.append(df_unmatched['Index'][i])
                    data["Min_Error"].append(df_unmatched['BlastedTime'][i] -
                                             df_matched_accurate[
                                                 "BlastedTime"][l])

        df_matched_approx_00 = pd.DataFrame(data).drop_duplicates()
        df_matched_approx_00 = df_matched_approx_00.loc[df_matched_approx_00.
            groupby(['Index'],
                    sort=False)
        ['Min_Error'].idxmin()]
        df_unmatched = df_pitram.drop(list(set(index2 + index1)))
        df_unmatched.reset_index(drop=True, inplace=True)
        return df_matched_approx_00, index2, df_unmatched

    def merge_on_predictive_mode_approximate(self, df, df_unmatched, ):
        """Iteration #3 - Finding coordinate of blast based on predictive mode.

            The coordinates of accurate 2 adjacent blasts can make a 3D
            line.Or an accurate blast's "from" and "to"
            [connect_cavecad] coordinates will make 3D line either. As
            the majority of the drives are straight, it is safe
            to assume that blast with chainage and without coordinate
            can be predicted using definition of point in
            3D line - see func() below. If the unknown blasts's chainage is
            within within 30m range [chainge_range] of
            known blasts chainage, it is considered here. Once the
            Geotechnical Data Management team process the blast,
            this approximate coordinate can be replaced by accurate blast
            coordinate in next iteration.
            Note: even if it is ramp, the 30m still is reasonable range
            for deviation]

        :raises [ErrorType]: [ErrorDescription]
        :return: tuple (df_matched_approx_00 - approximately matched set of
         blasts, index3 - index of them in initial connect_pitram
         [df_pitram] to exclude them in next iterations, df_unmatched -
         remaining unmatched blast_logs in connect_pitram [pitram])
        :rtype: tuple(df_matched_approx_01, list, pd.DataFrame)
        """

        def func(x1, y1, z1, x2, y2, z2, m1, m2, m3):
            """Calculating point in 3D line using distance between points
            and coordinate of 2 points.

            :param x1:  A float, Easting of the known coorinate #1
            :type x1: float, required
            :param y1: A float, Northing of the known coorinate #1
            :type y1: float, required
            :param z1: A float, Elevation of the known coorinate #1
            :type z1: float, required
            :param x2: A float, Easting of the known coorinate #2
            :type x2: float, required
            :param y2: A float, Northing of the known coorinate #2
            :type y2: float, required
            :param z2: A float, Elevation of the known coorinate #2
            :type z2: float, required
            :param m1: A float, chainage of the known coordinate #1
            :type m1: float, required
            :param m2: A float, chainage of the known coordinate #2
            :type m2: float, required
            :param m3: A float, chainage of the unknown coordinate #2
            :type m3: float, required
            :raises [ErrorType]: [ErrorDescription]
            :return: predicted coordinate sof unknown blast
            :rtype: tuple (Easting, Northing, Elevation)
            """
            dist = np.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2 + (z1 - z2) ** 2)
            v = np.array([(x1 - x2), (y1 - y2), (z1 - z2)]) / dist
            if (m1 >= m3) and (m2 >= m3) and (m1 > m2):
                d = m2 - m3
                prod = np.array([x2, y2, z2]) - d * v
            elif (m1 >= m3) and (m2 >= m3) and (m1 < m2):
                d = m1 - m3
                prod = np.array([x1, y1, z1]) - d * v
            elif (m1 <= m3) and (m2 >= m3):
                d = m3 - m1
                prod = np.array([x1, y1, z1]) + d * v
            elif (m1 >= m3) and (m2 <= m3):
                d = m3 - m2
                prod = np.array([x2, y2, z2]) + d * v
            elif (m1 <= m3) and (m2 <= m3) and (m1 < m2):
                d = m2 - m3
                prod = np.array([x2, y2, z2]) + d * v
            elif (m1 <= m3) and (m2 <= m3) and (m1 > m2):
                d = m1 - m3
                prod = np.array([x1, y1, z1]) + d * v
            else:
                prod = np.full(3, np.nan)
            return prod

        # Finding 2 closest coordinate with blast with unknown coordinate
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
                if (df_unmatched['Location_modified'][l] ==
                    df['Location_modified'][i]) and \
                        (abs(df_unmatched['MaxChainage'][l] - df['ChainageTo'][
                            i]) <
                         self.chainage_range):
                    data['CycleLocation'].append(
                        df_unmatched['CycleLocation'][l])
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
                    data["Min_Error"].append(
                        abs(df_unmatched['MaxChainage'][l] -
                            df['ChainageTo'][i]))

        df_missed01 = pd.DataFrame(data)
        # Picking closest 2 blasts in the drive from the unknown blasts
        df_missed02 = df_missed01.sort_values('Min_Error'). \
            groupby("Index", sort=False, as_index=False).nth(0)
        df_missed03 = df_missed01.sort_values('Min_Error'). \
            groupby("Index", sort=False, as_index=False).nth(1)
        # Merging them into one pd.DataFrame
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
        index3 = []

        for i in range(0, len(df_missed04)):
            for l in range(0, len(df_unmatched)):
                if (len(df_missed04[df_missed04.Index ==
                                    df_missed04["Index"][i]]) == 2) \
                        and (
                        df_unmatched["Index"][l] == df_missed04["Index"][i]) \
                        and ((df_missed04[
                    df_missed04.Index == df_missed04["Index"][i]]
                        .reset_index()['ChainageTo'][0]) != (
                             df_missed04[df_missed04.Index ==
                                         df_missed04["Index"][i]].
                                     reset_index()['ChainageTo'][1])) \
                        and (df_unmatched['MaxChainage'][l] >= 0):
                    function = func(df_missed04[df_missed04.Index ==
                                                df_missed04["Index"][
                                                    i]].reset_index()['ToX'][
                                        0],
                                    df_missed04[df_missed04.Index ==
                                                df_missed04["Index"][
                                                    i]].reset_index()['ToY'][
                                        0],
                                    df_missed04[df_missed04.Index ==
                                                df_missed04["Index"][
                                                    i]].reset_index()['ToZ'][
                                        0],
                                    df_missed04[df_missed04.Index ==
                                                df_missed04["Index"][
                                                    i]].reset_index()['ToX'][
                                        1],
                                    df_missed04[df_missed04.Index ==
                                                df_missed04["Index"][
                                                    i]].reset_index()['ToY'][
                                        1],
                                    df_missed04[df_missed04.Index ==
                                                df_missed04["Index"][
                                                    i]].reset_index()['ToZ'][
                                        1],
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
                    data['CycleLocation'].append(
                        df_unmatched['CycleLocation'][l])
                    data['ChainageFrom'].append(np.nan)
                    data['ChainageTo'].append(np.nan)
                    data['Watergel'].append(df_unmatched['Watergel'][l])
                    data['BlastedTime'].append(df_unmatched['BlastedTime'][l])
                    data["Correctness"].append('Approximate')
                    data["Index"].append(df_unmatched["Index"][l])
                    index3.append(df_unmatched['Index'][l])
                    data["Min_Error"].append("0")
                elif (len(df_missed04[
                              df_missed04.Index == df_missed04["Index"][
                                  i]]) == 1) \
                        and (df_unmatched["Index"][l] == df_missed04["Index"][
                    i]) and \
                        (df_unmatched['MaxChainage'][l] >= 0):
                    function = func(df_missed04[df_missed04.Index ==
                                                df_missed04["Index"][
                                                    i]].reset_index()['ToX'][
                                        0],
                                    df_missed04[df_missed04.Index ==
                                                df_missed04["Index"][
                                                    i]].reset_index()['ToY'][
                                        0],
                                    df_missed04[df_missed04.Index ==
                                                df_missed04["Index"][
                                                    i]].reset_index()['ToZ'][
                                        0],
                                    df_missed04[df_missed04.Index ==
                                                df_missed04["Index"][
                                                    i]].reset_index()['FromX'][
                                        0],
                                    df_missed04[df_missed04.Index ==
                                                df_missed04["Index"][
                                                    i]].reset_index()['FromY'][
                                        0],
                                    df_missed04[df_missed04.Index ==
                                                df_missed04["Index"][
                                                    i]].reset_index()['FromZ'][
                                        0],
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
                    data['CycleLocation'].append(
                        df_unmatched['CycleLocation'][l])
                    data['ChainageFrom'].append(np.nan)
                    data['ChainageTo'].append(np.nan)
                    data['Watergel'].append(df_unmatched['Watergel'][l])
                    data['BlastedTime'].append(df_unmatched['BlastedTime'][l])
                    data["Correctness"].append('Approximate')
                    data["Index"].append(df_unmatched["Index"][l])
                    index3.append(df_unmatched['Index'][l])
                    data["Min_Error"].append("0")

        df_matched_approx_01 = pd.DataFrame(data).drop_duplicates()
        df_matched_approx_01 = df_matched_approx_01.groupby("Index",
                                                            sort=False,
                                                            as_index=False).nth(
            0)

        df_pitram = self.df_pitram
        index1 = self.index1
        index2 = self.index2
        df_unmatched = df_pitram.drop(list(set(index3 + index1 + index2)))
        df_unmatched.reset_index(drop=True, inplace=True)

        return df_matched_approx_01, index3, df_unmatched

    def application(self):

        logger.info('loading data from the Pitram database')
        try:
            df_pitram = self.connect_pitram(self)
        except OperationalError as e:
            logger.error('error loading data from Pitram... aborting')
            return

        logger.info('loading data from CaveCAD')
        try:
            df = self.connect_cavecad(self, df_pitram)
        except OperationalError as e:
            logger.error('error loading data from Cavecad... abortin')

        df_pitram = self.append_unmatched_approximate(self)

        df_matched_accurate, index1, df_unmatched = \
            self.merge_on_chainage_exact(self, df_pitram, df)

        df_matched_approx_00, index2, df_unmatched  \
            = self.merge_on_matched_date_approximate(self,
                                                     df_matched_accurate,
                                                     df_pitram, index1)



        self.df_matched_approx_01 = merge_on_predictive_mode_approximate(self)[0]
        self.index3 = merge_on_predictive_mode_approximate(self)[1]
        self.df_unmatched = merge_on_predictive_mode_approximate(self)[2]

        def merge_on_cavecad_processed_date_approximate(self):
            """Iteration #4 - approximate matching unmatched blasts [
            connect_pitram] on connect_cavecad.
                Heading name must match and blasted date must be within
                days_range [scenario: 5 heading in 2 weeks = 25m]

                The day, Data Management team finished processing the
                scanning, was logged in CaveCad as
                "ExcavationDate". Even though there are "Date Fired"
                attribute, it has shown inconsistent
                formatting and varying accuracy while "ExcavationDate" was
                appended without human interference.
                Therefore, "BlastedTime" [connect_pitram]and
                "ExcavationDate" were matched.

                Iterations are finished here: Results from all iterations
                are combined here.

            :raises [ErrorType]: [ErrorDescription]
            :return: tuple (df_unmatched - left unmatched blasts from
            connect pitram, df_matched_all - combining all blasts with
            coordinate)
            :rtype: tuple(df_unmatched, df_matched_all)
            """
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
            index4 = []
            df_unmatched = self.df_unmatched
            df = self.df
            df['ExcavationDate'] = pd.to_datetime(df['ExcavationDate'])
            for i in range(0, len(df_unmatched)):
                for l in range(0, len(df)):
                    if (df['Location_modified_exact'][l] == df_unmatched['Location_modified_exact'][i])\
                        & ((pd.Timestamp(df_unmatched['BlastedTime'][i]) - pd.Timestamp(df["ExcavationDate"][l])) <
                            timedelta(days=+self.days_range))\
                            & ((pd.Timestamp(df_unmatched['BlastedTime'][i]) - pd.Timestamp(df["ExcavationDate"][l])) >
                                timedelta(days=0)):
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
                        index4.append(df_unmatched['Index'][i])
                        data["Min_Error"].append(df_unmatched['BlastedTime'][i] - df["ExcavationDate"][l])

            df_matched_approx_02 = pd.DataFrame(data).drop_duplicates()
            df_matched_approx_02 = df_matched_approx_02.loc[df_matched_approx_02.
                                                            groupby(['Index'],
                                                                    sort=False)
                                                            ['Min_Error'].idxmin()]

            index1 = self.index1
            index2 = self.index2
            index3 = self.index3
            df_pitram = self.df_pitram
            df_unmatched = df_pitram.drop(list(set(index4 + index3 + index2 + index1)))
            df_unmatched.reset_index(drop=True, inplace=True)

            df_unmatched['UniqueID'] = list(map(lambda date, location: (str(date) + '/' +
                                                                        location), df_unmatched['BlastedTime'],
                                                                        df_unmatched['CycleLocation']))

            df_matched_accurate = self.df_matched_accurate
            df_matched_approx_00 = self.df_matched_approx_00
            df_matched_approx_01 = self.df_matched_approx_01
            df_matched_all = df_matched_accurate.append([
                df_matched_approx_00, df_matched_approx_01,
                df_matched_approx_02], sort=False)
            df_matched_all.reset_index(drop=True, inplace=True)


            df_matched_all['UniqueID'] = list(map(lambda date, location: (
                    str(date) + '/' + location), df_matched_all[
                'BlastedTime'], df_matched_all['CycleLocation']))

            return df_unmatched, df_matched_all
        self.df_unmatched = merge_on_cavecad_processed_date_approximate(self)[0]
        self.df_matched_all = merge_on_cavecad_processed_date_approximate(self)[1]
        def write_to_sql(self):
            """
            writing results to blast_log, SQLite db.
            :raises [ErrorType]: [ErrorDescription]
            :return: modified/created blast_log SQLite db.
            :rtype: SQLite DB
            """
            engine = create_engine("sqlite:///" + self.input_dir, echo=False)
            conn = engine.connect()
            df_matched_all = self.df_matched_all
            df_unmatched = self.df_unmatched
            for i in range(0, len(df_matched_all)):
                conn.execute('REPLACE into Matched_Blasts values (?,?,?,?,?,'
                             '?,?,?,?,?)',
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
                conn.execute('REPLACE into Unmatched_Blasts values (?,?,?,?,'
                             '?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
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
            conn.begin().commit()
            conn.close()
            engine.dispose()
            self.end_time= datetime.now().replace(tzinfo=tzoffset('uln',
                                                                  8 * 3600))
            return print("TIME to update SQLite: {}".format(self.end_time -
                                                            self.start_time))
        return print("Successfully updated SQLite database"), write_to_sql(
            self)