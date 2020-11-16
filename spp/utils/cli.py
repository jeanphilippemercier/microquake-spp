"""
CLI-related classes and functions
"""


import argparse
import importlib.util

from microquake.helpers.logging import logger

from microquake.core.settings import settings


class CLI:
    """
    CLI allows us to run the various modules in a consistent way, and handle args
    in a predictable manner across all SPP modules.
    """

    def __init__(
        self,
        module_name,
        processing_flow_name="automatic",
        settings_name=None,
        app=None,
        args=None
    ):
        self.module_name = module_name
        self.processing_flow_name = processing_flow_name
        self.settings_name = settings_name
        self.app = app
        self.args = args

        if not args:
            self.process_arguments()

        self.set_defaults()

        if not self.module_name:
            print("No module specified, exiting")
            exit()

        self.set_app()

        if not self.app:
            print("No application mode specified, exiting")
            exit()

        # If we're running a chain of modules, no need to load the module and settings

        if self.args.modules:
            return

        self.load_module(self.module_name, self.settings_name)

        if not self.module_settings:
            print("Module name {} not found in settings and not running module chain, exiting".format(module_name))
            exit()

    def process_arguments(self):
        """
        Define and process cli arguments
        """

        parser = argparse.ArgumentParser(description="Run an SPP module")
        parser.add_argument("--module", help="the name of the module to run")
        parser.add_argument("--settings_name", help="to override the name of the module when loading settings")
        parser.add_argument("--processing_flow", help="the name of the processing flow to run")
        parser.add_argument("--once", help="stop a module after a message has been processed", type=bool)
        parser.add_argument(
            "--modules", help="a list of modules to chain together"
        )
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

    def load_module(self, module_name, settings_name):
        self.module_settings = settings.get(settings_name)

        spec = importlib.util.spec_from_file_location(
            "spp.pipeline." + module_name, "./spp/pipeline/" + module_name + ".py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        processor = getattr(mod, 'Processor')
        self.processor = processor(app=self.app,
                                   module_name=module_name)

    def set_defaults(self):
        # If there is a CLI arg for module_name, use it!

        if self.args.module and not self.module_name:
            self.module_name = self.args.module

        # If we are running a chain of modules, use the name 'chain'

        if self.args.modules and not self.module_name:
            self.module_name = 'chain'

        # If there is a CLI arg for settings_name, use it! Otherwise use module_name

        if self.args.settings_name and not self.settings_name:
            self.settings_name = self.args.settings_name
        elif not self.args.settings_name and not self.settings_name:
            self.settings_name = self.module_name

        if self.args.processing_flow and not self.processing_flow_name:
            self.processing_flow_name = self.args.processing_flow

    def set_app(self):
        if self.app:
            return

        if self.args.mode == "cont":
            from spp.utils.kafka_redis_application import KafkaRedisApplication

            self.app = KafkaRedisApplication(
                module_name=self.module_name,
                processing_flow_name=self.processing_flow_name,
            )
        elif self.args.mode == "local":
            from spp.utils.local_application import LocalApplication

            self.app = LocalApplication(
                module_name=self.module_name,
                processing_flow_name=self.processing_flow_name,
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
                processing_flow_name=self.processing_flow_name,
                event_id=self.args.event_id,
                send_to_api=self.args.send_to_api,
            )

    def run_module_chain(self, modules, msg_in):
        cat, stream = self.app.deserialise_message(msg_in)

        for module_file_name in modules:
            self.load_module(module_file_name, module_file_name)

            cat, stream = self.app.clean_message((cat, stream))

            res = self.processor.process(cat=cat, stream=stream)

            cat_out, st_out = self.processor.legacy_pipeline_handler(msg_in, res)

        return cat, stream

    def run_module_continuously(self):
        for msg_in in self.app.consumer_msg_iter():
            try:
                if self.args.modules:
                    cat, st = self.run_module_chain(
                        self.args.modules.split(","), msg_in
                    )
                else:
                    cat, st = self.app.receive_message(
                        msg_in,
                        self.processor,
                    )
            except Exception as e:
                logger.error(e, exc_info=True)

                continue
            self.app.send_message(cat, st)

            if self.args.once:
                return

    def run_module_locally(self):
        msg_in = self.app.get_message()

        if self.args.modules:
            cat, st = self.run_module_chain(
                self.args.modules.split(","), msg_in
            )
        else:
            cat, st = self.app.receive_message(
                msg_in,
                self.processor,
            )
        self.app.send_message(cat, st)

    def run_module_with_api(self):
        msg_in = self.app.get_message()

        if self.args.modules:
            cat, st = self.run_module_chain(
                self.args.modules.split(","), msg_in
            )
        else:
            cat, st = self.app.receive_message(
                msg_in,
                self.processor,
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
            logger.error(
                "Running module in unknown mode %s", self.args.mode
            )

        self.app.close()
