from spp.utils.cli import CLI


def main():
    cli = CLI("chain")
    if cli.args.modules:
        cli.run_module()
    else:
        cli.app.logger.warning("modules arg not passed, modules in chain not specified, exiting")

if __name__ == "__main__":
    main()
