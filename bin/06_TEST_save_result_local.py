#!/usr/bin/env python3
# This modules expect to receive a message containing the following:
# [catalog, stream, context_stream, event_id]

import os
from io import BytesIO
from time import time

import matplotlib.pyplot as plt
import numpy as np
from scipy.signal import hilbert
from tqdm import tqdm

from microquake.core import Stream
from microquake.io import msgpack
from spp.utils import seismic_client
from spp.utils.cli import CLI
from spp.utils.seismic_client import post_data_from_objects


def process(
    cat=None,
    stream=None,
    logger=None,
    app=None,
    module_settings=None,
    prepared_objects=None,
):
    logger.info("saving results to disk")

    spp_home = os.environ["SPP_HOME"]

    # origin[0] is used here to ensure that the filename is consistent
    # throughout.

    fname = str(cat[0].origins[0].time) + ".xml"

    fpath = os.path.join(spp_home, "results", fname)

    cat.write(fpath, format="QUAKEML")
    # stream.write(fpath.replace('xml', 'mseed'), format='MSEED')

    ev_loc_auto = cat[0].origins[-1].loc
    ev_loc_manual = cat[0].origins[0].loc

    trs_man = []
    trs_auto = []
    starttimes = []
    endtimes = []
    for tr in tqdm(stream):
        station = tr.stats.station
        ptt_auto_p = app.get_grid_point(station, "P", ev_loc_auto)
        ptt_auto_s = app.get_grid_point(station, "S", ev_loc_auto)
        ptt_man_p = app.get_grid_point(station, "P", ev_loc_manual)
        ptt_man_s = app.get_grid_point(station, "S", ev_loc_manual)
        tr2_p = tr.copy()
        tr2_s = tr.copy()
        tr3_p = tr.copy()
        tr3_s = tr.copy()
        tr2_p.stats.starttime -= ptt_auto_p
        tr2_s.stats.starttime -= ptt_auto_s
        tr3_p.stats.starttime -= ptt_man_p
        tr3_s.stats.starttime -= ptt_man_s
        starttimes.append(tr2_s.stats.starttime)
        starttimes.append(tr3_s.stats.starttime)
        endtimes.append(tr2_p.stats.endtime)
        endtimes.append(tr3_p.stats.endtime)
        trs_auto.append(tr2_p)
        trs_auto.append(tr2_s)
        trs_man.append(tr3_p)
        trs_man.append(tr3_s)

    st_auto = (
        Stream(traces=trs_auto)
        .normalize()
        .trim(
            starttime=np.min(starttimes),
            endtime=np.max(endtimes),
            fill_value=0,
            pad=True,
        )
    )

    st_auto.filter("bandpass", freqmin=100, freqmax=200)
    st_man = (
        Stream(traces=trs_man)
        .normalize()
        .trim(
            starttime=np.min(starttimes),
            endtime=np.max(endtimes),
            fill_value=0,
            pad=True,
        )
    )
    st_man.filter("bandpass", freqmin=100, freqmax=200)

    lentr = len(st_auto[0].data) - 100
    stack_auto = np.zeros(lentr)
    im_auto = []
    for tr in st_auto:
        data = np.nan_to_num(tr.data[:lentr])
        # data /= np.max(np.abs(data))
        # stack_auto += np.abs(hilbert(np.nan_to_num(data)))
        stack_auto += np.nan_to_num(data) ** 2
        im_auto.append(data)

    # lentr = len(st_man[0].data) - 100
    stack_man = np.zeros(lentr)
    im_man = []
    for tr in st_man:
        data = np.nan_to_num(tr.data[:lentr])
        # data /= np.max(np.abs(data))
        # stack_man += np.abs(hilbert(np.nan_to_num(data)))
        stack_man += np.nan_to_num(data) ** 2
        im_man.append(data)

    odir = os.path.join(os.environ["SPP_HOME"], "results")

    t = np.arange(len(stack_auto)) / 6000
    plt.close("all")
    plt.figure(3, figsize=(20, 4))
    plt.clf()
    plt.plot(t, np.abs(stack_auto), "r", alpha=0.5, label="automatic")
    plt.plot(t, np.abs(stack_man), "b", alpha=0.5, label="IMS manual")
    plt.legend()
    plt.tight_layout()

    plt.savefig(os.path.join(odir, "%s_stacked.pdf" % str(cat[0].origins[0].time)))

    dist = np.linalg.norm(ev_loc_auto - ev_loc_manual)
    # for tr in st:
    #     tr.data = tr.data ** 4 * np.sign(tr.data)

    residuals = []
    for arrival in cat[0].preferred_origin().arrivals:
        residuals.append(arrival.time_residual)

    plt.figure(1)
    plt.clf()
    stream.distance_time_plot(cat[0], app.get_stations(), scale=15)
    plt.title(
        "Automatic (AM distance=%d m, mean residual=%0.4f s)"
        % (dist, np.mean(np.abs(residuals)))
    )
    plt.tight_layout()
    plt.savefig(
        os.path.join(odir, "%s_automatic.png" % str(cat[0].origins[0].time)), dpi=200
    )

    plt.figure(2)
    plt.clf()
    cat[0].preferred_origin_id = cat[0].origins[1].resource_id
    for arrival in cat[0].preferred_origin().arrivals:
        residuals.append(arrival.time_residual)

    stream.distance_time_plot(cat[0], app.get_stations(), scale=15)
    plt.title("Manual (mean residual: %0.4f s)" % np.mean(np.abs(residuals)))
    plt.tight_layout()
    plt.savefig(
        os.path.join(odir, "%s_manual.png" % str(cat[0].origins[0].time)), dpi=200
    )

    return cat, stream


__module_name__ = "event_database"


def main():
    cli = CLI(__module_name__, callback=process)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
