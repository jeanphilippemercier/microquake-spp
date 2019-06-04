from spp.utils.cli import CLI

__module_name__ = "ray_tracer"


def main():
    cli = CLI(__module_name__, processing_flow_name='ray_tracing')
    cli.run_module()


if __name__ == "__main__":
    main()
