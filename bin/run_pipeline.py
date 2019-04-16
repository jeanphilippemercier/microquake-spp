"""
This script is used to run the spp pipeline

CLI arguments are available to run the pipeline in a few different ways:

- A long-running worker mode passing messages through kafka & redis
- A mode to process local input once, optionally saving the results
- A mode to process API data, optionally posting it back to the API

You can run modules manually with input/output mseed and quakeml files.
python ./bin/run_pipeline.py --module=picker --mode=local --input_mseed=./tmp/input.mseed --input_quakeml=./tmp/input.xml --output_mseed=./tmp/output.mseed --output_quakeml=./tmp/output.xml

In production, we run specific modules in a long-running worker mode, picking msgs off kafka & redis:
python ./bin/run_pipeline.py --module=picker --processing_flow=automatic --mode=cont

Msgpack files are serialised, binary representations of both the catalog and waveform objects.
You can run modules with these files directly.
python ./bin/run_pipeline.py --module=picker --mode=local --input_bytes=./tmp/input.msgpack --output_bytes=./tmp/output.msgpack

You can run a module with data from the API:
python ./bin/run_pipeline.py --module=picker --mode=api --event_id=smi:local/97f39d25-db59-40fb-bcf8-57de70589fd1

And then optionally post the data back to the API with --send_to_api=True
python ./bin/run_pipeline.py --module=picker --mode=api --send_to_api=True --event_id=smi:local/97f39d25-db59-40fb-bcf8-57de70589fd1

You are also able to run a set of different modules at once, piping data from one to the next
python ./bin/run_pipeline.py --mode=local --modules=interloc,picker --processing_flow=automatic --input_mseed=./tmp/input.mseed --input_quakeml=./tmp/input.xml

"""
from spp.utils.cli import CLI


def main():
    cli = CLI(None, None)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
