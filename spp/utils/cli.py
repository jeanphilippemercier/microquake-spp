"""
CLI-related classes and functions
"""


import argparse

from spp.utils.application import Application


class CLI:
    """
    CLI allows us to run the various modules in a consistent way, and handle args
    in a predictable manner across all SPP modules.
    """

    def __init__(
        self,
        module_name,
        processing_flow_name="automatic",
        callback=None,
        prepare=None,
    ):
        self.module_name = module_name
        self.processing_flow_name = processing_flow_name
        self.callback = callback
        self.prepare = prepare
        self.prepared_objects = {}

        self.process_arguments()

        self.app = Application(
            module_name=self.module_name, processing_flow_name=processing_flow_name
        )
        self.app.init_module()

        self.module_settings = self.app.settings[module_name]

    def process_arguments(self):
        """
        Define and process cli arguments
        """

        parser = argparse.ArgumentParser(description="Run an SPP module")
        parser.add_argument("--module_name", help="the name for the module")
        args = parser.parse_args()
        if args.module_name:
            self.module_name = args.module_name

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

    def run_module(self):
        """
        Run the module by calling the function passed into init as "callback"
        Here we also wrap this function so that we can call it continuously
        whenever we receive new data from kafka.
        """

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
                self.app.logger.error(e)
                continue
            self.app.send_message(cat, st)

        self.app.close()
