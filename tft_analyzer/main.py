import click

from common.services.pipeline import run


@click.group()
def cli() -> None:
    pass


def main() -> None:
    cli.add_command(run)
    cli.main(standalone_mode=False)


if __name__ == "__main__":
    main()
