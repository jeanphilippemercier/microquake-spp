from spp.utils.cli import CLI
from spp.pipeline.ray_tracer import process

__module_name__ = "ray_tracer"


def main():
    cli = CLI(__module_name__, processing_flow_name='ray_tracing',
              callback=process)
    cli.run_module()


if __name__ == "__main__":
    main()
