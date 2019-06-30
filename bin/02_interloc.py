from spp.utils.cli import CLI

__module_name__ = "interloc"


def main():
    cli = CLI(__module_name__)
    cli.run_module()


if __name__ == "__main__":
    main()
