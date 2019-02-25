"""
CLI-related classes and functions

The default mode is to run continuously, listening for messages on the message bus:
python ./bin/03_picker.py

Setting this explicitly:
python ./bin/03_picker.py --mode=cont

You can run modules manually with input/output mseed and quakeml files.
python ./bin/03_picker.py --mode=local --input_mseed=./tmp/input.mseed --input_quakeml=./tmp/input.xml --output_mseed=./tmp/output.mseed --output_quakeml=./tmp/output.xml

Msgpack files are serialised, binary representations of both the catalog and waveform objects.
You can run modules with these files directly.
python ./bin/03_picker.py --mode=local --input_bytes=./tmp/input.msgpack --output_bytes=./tmp/output.msgpack

You can run a module with data from the API:
python ./bin/03_picker.py --mode=api --event_id=smi:local/97f39d25-db59-40fb-bcf8-57de70589fd1

And then optionally post the data back to the API with --send_to_api=True
python ./bin/03_picker.py --mode=api --send_to_api=True --event_id=smi:local/97f39d25-db59-40fb-bcf8-57de70589fd1
"""


import argparse


class CLI:
    """
    CLI allows us to run the various modules in a consistent way, and handle args
    in a predictable manner across all SPP modules.
    """

    def __init__(
        self, module_name, processing_flow_name="automatic", callback=None, prepare=None
    ):
        self.module_name = module_name
        self.processing_flow_name = processing_flow_name
        self.callback = callback
        self.prepare = prepare
        self.prepared_objects = {}

        self.process_arguments()

        if self.args.mode == "cont":
            from spp.utils.kafka_redis_application import KafkaRedisApplication

            self.app = KafkaRedisApplication(
                module_name=self.module_name, processing_flow_name=processing_flow_name
            )
        elif self.args.mode == "local":
            from spp.utils.local_application import LocalApplication

            self.app = LocalApplication(
                module_name=self.module_name,
                processing_flow_name=processing_flow_name,
                input_bytes=self.args.input_bytes,
                input_mseed=self.args.input_mseed,
                input_quakeml=self.args.input_quakeml,
                output_bytes=self.args.output_bytes,
                output_mseed=self.args.output_mseed,
                output_quakeml=self.args.output_quakeml,
            )
        elif self.args.mode == "api":
            from spp.utils.api_application import APIApplication

            self.app = APIApplication(
                module_name=self.module_name,
                processing_flow_name=processing_flow_name,
                event_id=self.args.event_id,
                send_to_api=self.args.send_to_api,
            )
        else:
            print("No application mode specified, exiting")
            exit()

        self.module_settings = self.app.settings[module_name]

    def process_arguments(self):
        """
        Define and process cli arguments
        """

        parser = argparse.ArgumentParser(description="Run an SPP module")
        parser.add_argument("--module_name", help="the name for the module")
        parser.add_argument(
            "--mode",
            choices=["local", "cont", "api"],
            default="cont",
            help="the mode to run this module in. Options are local, cont (for continuously running modules), and api",
        )
        parser.add_argument("--input_bytes")
        parser.add_argument("--input_mseed")
        parser.add_argument("--input_quakeml")
        parser.add_argument("--output_bytes")
        parser.add_argument("--output_mseed")
        parser.add_argument("--output_quakeml")
        parser.add_argument("--event_id")
        parser.add_argument("--send_to_api", type=bool)

        self.args = parser.parse_args()

    def prepare_module(self):
        """
        Some modules require running some code at startup, separate of processing
        we pass a function into the init as "prepare" and call that here
        """
        if self.prepare is None:
            self.app.logger.info(
                "no preparation function for module %s", self.module_name
            )
            return

        self.app.logger.info("preparing module %s", self.module_name)
        self.prepared_objects = self.prepare(self.app, self.module_settings)
        self.app.logger.info("done preparing module %s", self.module_name)

    def run_module_continuously(self):
        for msg_in in self.app.consumer_msg_iter():
            try:
                cat, st = self.app.receive_message(
                    msg_in,
                    self.callback,
                    app=self.app,
                    prepared_objects=self.prepared_objects,
                    module_settings=self.module_settings,
                )
            except Exception as e:
                self.app.logger.error(e, exc_info=True)
                continue
            self.app.send_message(cat, st)

    def run_module_locally(self):
        msg_in = self.app.get_message()
        cat, st = self.app.receive_message(
            msg_in,
            self.callback,
            app=self.app,
            prepared_objects=self.prepared_objects,
            module_settings=self.module_settings,
        )
        self.app.send_message(cat, st)

    def run_module_with_api(self):
        msg_in = self.app.get_message()
        cat, st = self.app.receive_message(
            msg_in,
            self.callback,
            app=self.app,
            prepared_objects=self.prepared_objects,
            module_settings=self.module_settings,
        )
        self.app.send_message(cat, st)

    def run_module(self):
        """
        Run the module by calling the function passed into init as "callback"
        """

        if self.args.mode == "cont":
            self.run_module_continuously()
        elif self.args.mode == "local":
            self.run_module_locally()
        elif self.args.mode == "api":
            self.run_module_with_api()
        else:
            self.app.logger.error("Running module in unknown mode %s", self.args.mode)

        self.app.close()
