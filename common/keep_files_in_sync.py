import pickle
import hashlib
import os

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARN)
#logger.setLevel(logging.INFO)

from spp.utils.application import Application
from microquake.core.data.write_ot import write_OT_xml

"""
    This is a first step ... likely to be rewritten pretty fast.
    The idea is that we have several files that need to be 'in-sync':
        sensors.csv, cables.csv, cable_types.csv, OT.xml, NLL/run/*,
        NLL/time/all_the_slowness_grids_for_each_stn, etc.

    Here are a couple of scenarios:
        a. Analyst changed a csv file -->  1. stationXML (OT.xml) must be updated
                                           2. nlloc control files must be updated
                                           3. nlloc time/ slowness grids must be updated
                                              (note this step is slow!)

        b. Analyst changes the velocity model -->  1. nlloc control files must be updated
                                                   2. nlloc time/ slowness grids must be updated
                                                      (note this step is slow!)


    Currently (2019-03-15) we're not tracking the velocity model ids (.rid) well
    enough to track/trigger on the second event above.

    However, for the first case, a quick and dirty solution is to track checksums
    of the files here and recreate if they change
"""

class file_holder():
    pass


# For now this is just a local file on disk:
SHA_DB_FILE = "DB"


def main():


    if 'SPP_COMMON' not in os.environ or 'SPP_CONFIG' not in os.environ:
        logger.error("Set your SPP envs!")
        exit(2)
    path = os.environ['SPP_COMMON']


    sensor_file = os.path.join(path, 'sensors.csv')
    sensor_types_file = os.path.join(path,'sensor_types.csv')
    cables_file = os.path.join(path, 'cables.csv')

    # reading application data
    app = Application()
    settings = app.settings
    xml_outfile = os.path.join(path, settings.sensors.stationXML)

    files_to_track = [sensor_file, sensor_types_file, cables_file, xml_outfile]

    files = file_holder()
    files.sensor_file = sensor_file
    files.sensor_types_file = sensor_types_file
    files.cables_file = cables_file
    files.xml_outfile = xml_outfile


    try:
        with open(SHA_DB_FILE, 'rb') as handle:
            l = pickle.load(handle)
    except IOError:
        l = []

    db = dict(l)


    create_new_stationxml = False
    create_new_nllocgrids = False

    for file in files_to_track:

        checksum = hashlib.md5(open(file).read().encode('utf-8')).hexdigest()
        print("file:%s checksum:%s" % (file, checksum))

        if db.get(file, None) != checksum:
            print("file:%s checksum:%s != db.checksum:%s" % (file, checksum, db.get(file,None)))

            db[file] = checksum
            create_new_stationxml = True
            create_new_nllocgrids = True

        else:
            print("file:%s HAS NOT changed" % file)


    if create_new_stationxml:
        update_stationxml(files)
        db[xml_outfile] = hashlib.md5(open(xml_outfile).read().encode('utf-8')).hexdigest()

    if create_new_nllocgrids:
        update_nllocgrids(app)

    update_db(db)

    return

def update_stationxml(files):

    import datetime
    x = datetime.datetime.now()
    suff = x.strftime("%x_%X")
    suff = x.strftime("%Y-%m-%dT%H:%M:%S")

    xml_outfile = files.xml_outfile

    import shutil
    shutil.copy(xml_outfile, xml_outfile + suff)

    success = write_OT_xml(files.sensor_file, files.sensor_types_file,
                           files.cables_file, xml_outfile=files.xml_outfile,
                           logger_in=logger)

    assert success == 1

    return

def update_nllocgrids(app):
    from microquake.core.nlloc import NLL
    settings = app.settings
    params = settings.nlloc

    project_code = settings.project_code
    base_folder = settings.nlloc.nll_base
    gridpar = app.nll_velgrids()
    sensors = app.nll_sensors()

    logger.info('Preparing NonLinLoc')

    nll = NLL(project_code, base_folder=base_folder, gridpar=gridpar, sensors=sensors, params=params)
    nll.prepare(create_time_grids=True)

def update_db(db):
    with open(SHA_DB_FILE, 'wb') as handle:
        pickle.dump(db, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return


if __name__ == "__main__":
    main()


