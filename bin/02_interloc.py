from spp.utils.cli import CLI
from spp.pipeline.interloc import process

__module_name__ = "interloc"


def main():
    cli = CLI(__module_name__, callback=process)
    cli.run_module()


if __name__ == "__main__":
    main()
