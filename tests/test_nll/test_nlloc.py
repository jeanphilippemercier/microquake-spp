from microquake.core import ctl
import os

from microquake import nlloc

config_dir = os.environ['SPP_CONFIG']
config_file = config_dir + '\input.xml'
params = ctl.parse_control_file(config_file)

nll_opts = nlloc.init_nlloc_from_params(params)

nll_opts.prepare(create_time_grids=True, tar_files=False, SparkContext=None)



