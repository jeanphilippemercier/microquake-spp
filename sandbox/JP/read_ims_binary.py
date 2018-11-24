import struct
import numpy as np

with open('40_513674516_0050_0.s', 'rb') as fileobj:
    # fileobj.seek(-1)
    fileobj.seek(0)

    # def strided_read(content):
    # """
    # Efficiently read the content of the binary object returned by the IMS server
    # :param content: content of the binary object returned by IMS
    # :return: time and signal
    # """

    io = fileobj.read()

    # reading from the end of the buffer dividing into chunks of 20 bytes
    time = {}
    buf_len = 4
    # for offset in [0, 1, 2, 3, 4, 5, 6, 7]:
        #time[str(offset)] = []
    for k in np.arange(0, int(len(io)/12) - 12):
        ch1 = [k*buf_len:k*buf_len+buf_len]]

        data = io[k*buf_len:k*buf_len+buf_len]


    # for k in np.arange(0, (int(np.floor(len(io) / 20)))):
    #     data = io[-4*k-5:-4*k-1]
    #     time.append(struct.unpack('>i', data[0:4]))

    #
    #
    # npts = int(len(content) / 20)
    # time = np.ndarray((npts,), '>q', content, 0, (20, ))
    # sigs = np.zeros((3, npts), dtype=np.float32)
    # sigs[0] = np.ndarray((npts,), '>f', content, 8, (20, ))
    # sigs[1] = np.ndarray((npts,), '>f', content, 12, (20, ))
    # sigs[2] = np.ndarray((npts,), '>f', content, 16, (20, ))
    #
    # header_size = struct.unpack('>i', fileobj.read(4) )[0]
    # net_id = struct.unpack('>i', fileobj.read(4))[0]
    # site_id = struct.unpack('>i', fileobj.read(4))[0]
    # starttime = struct.unpack('>q', fileobj.read(8))[0]
    # endtime = struct.unpack('>q', fileobj.read(8))[0]
    # netADC_id = struct.unpack('>i', fileobj.read(4))[0]
    # sensor_id = struct.unpack('>i', fileobj.read(4))[0]
    # attenuator_id = struct.unpack('>i', fileobj.read(4))[0]
    # attenuator_config_id = struct.unpack('>i', fileobj.read(4))[0]


    # import requests
    # from gzip import GzipFile
    # import struct
    # import numpy as np
    # from microquake.core import Trace, Stream, UTCDateTime
    # import sys
    # from time import time as timer
    # from datetime import datetime
    #
    # if sys.version_info[0] < 3:
    #     from StringIO import StringIO
    # else:
    #     from io import StringIO, BytesIO
    #
    # if isinstance(site_ids, int):
    #     site_ids = [site_ids]
    #
    # start_datetime_utc = UTCDateTime(start_datetime)
    # end_datetime_utc = UTCDateTime(end_datetime)
    # reqtime_start_nano = int(start_datetime_utc.timestamp * 1e6) * int(1e3)
    # reqtime_end_nano = int(end_datetime_utc.timestamp * 1e6) * int(1e3)
    # url_cont = base_url + '/continuous-seismogram?' + \
    #            'startTimeNanos=%d&endTimeNanos=%d&siteId' + \
    #            '=%d&format=%s'
    #
    # stream = Stream()
    # for site in site_ids:
    #     ts_processing = timer()
    #
    #     if type(site) == str:
    #         site = int(site)
    #
    #     url = url_cont % (reqtime_start_nano, reqtime_end_nano, site, format)
    #     url = url.replace('//', '/').replace('http:/', 'http://')
    #
    #     logger.info("Getting trace for station %d\nstarttime: %s\n"
    #                 "endtime:   %s" % (site, start_datetime, end_datetime))
    #
    #     ts = timer()
    #     r = requests.get(url, stream=True)
    #
    #     if r.status_code != 200:
    #         raise Exception('request failed! \n %s' % url)
    #         continue
    #     if format == 'binary-gz':
    #         fileobj = GzipFile(fileobj=BytesIO(r.content))
    #     elif format == 'binary':
    #         fileobj = BytesIO(r.content)
    #     else:
    #         raise Exception('unsuported format!')
    #         continue
    #
    #     fileobj.seek(0)
    #     te = timer()
    #     logger.info('Completing request in %f seconds' % (te - ts))
    #
    #     # Reading header
    #     # try:
    #     if len(r.content) < 44:
    #         continue
    #     ts = timer()
    #     header_size = struct.unpack('>i', fileobj.read(4) )[0]
    #     net_id = struct.unpack('>i', fileobj.read(4))[0]
    #     site_id = struct.unpack('>i', fileobj.read(4))[0]
    #     starttime = struct.unpack('>q', fileobj.read(8))[0]
    #     endtime = struct.unpack('>q', fileobj.read(8))[0]
    #     netADC_id = struct.unpack('>i', fileobj.read(4))[0]
    #     sensor_id = struct.unpack('>i', fileobj.read(4))[0]
    #     attenuator_id = struct.unpack('>i', fileobj.read(4))[0]
    #     attenuator_config_id = struct.unpack('>i', fileobj.read(4))[0]
    #     te = timer()
    #     logger.info('Unpacking header in %f seconds' % (te - ts))
    #
    #     ts = timer()
    #     # Reading data
    #     fileobj.seek(header_size)
    #     content = fileobj.read()
    #
    #     time, sigs = strided_read(content)
    #
    #     time_norm = (time - time[0]) / 1e9
    #     tstart_norm_new = (reqtime_start_nano - time[0]) / 1e9
    #     tend_norm_new = (reqtime_end_nano - time[0]) / 1e9
    #     nan_ranges = get_nan_ranges(time_norm, sampling_rate, limit=nan_limit)
    #
    #     time_new = np.arange(time_norm[0], time_norm[-1], 1. / sampling_rate)
    #
    #     newsigs = np.zeros((len(sigs), len(time_new)), dtype=np.float32)
    #     for i in range(len(sigs)):
    #         newsigs[i] = np.interp(time_new, time_norm, sigs[i])
    #
    #     nan_ranges_ix = ((nan_ranges - time_new[0]) * sampling_rate).astype(int)
    #
    #     for chan in newsigs:
    #         for lims in nan_ranges_ix:
    #             chan[lims[0]:lims[1]] = np.nan
    #
    #     te = timer()
    #     logger.info("Unpacking data in %f seconds for %d points"
    #                 % (te - ts, len(time_new)))
    #
    #     chans = ['X', 'Y', 'Z']
    #
    #     for i in range(len(newsigs)):
    #         tr = Trace(data=newsigs[i])
    #         tr.stats.sampling_rate = sampling_rate
    #         tr.stats.network = str(network)
    #         tr.stats.station = str(site)
    #         # it seems that the time returned by IMS is local time...
    #         starttime_local = datetime.fromtimestamp(time[0]/1e9)
    #         starttime_local.replace(tzinfo=start_datetime.tzinfo)
    #         tr.stats.starttime = UTCDateTime(starttime_local)
    #         tr.stats.channel = chans[i]
    #         stream.append(tr)
    #
    #     te_processing = timer()
    #     logger.info("Processing completed in %f" % (te_processing -
    #                                                 ts_processing))
    #
    # return stream