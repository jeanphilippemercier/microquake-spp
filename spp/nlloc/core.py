

def prepare_nll(ctl_filename='input.xml', nll_base='NLL'):
    """
    :param ctl_filename: path to the XML file containing control parameters
    :param nll_base: directory in which NLL project will be built
    """
    params = ctl.parseControlFile(ctl_filename)
    keys = ['velgrids', 'sensors']
    for job_index, job in enumerate(ctl.buildJob(keys, params)):

        params = ctl.getCurrentJobParams(params, keys, job)
        nll_opts = init_from_xml_params(params, base_folder=nll_base)
        nll_opts.prepare(create_time_grids=True, tar_files=False)