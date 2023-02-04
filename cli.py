import click
from aggregator.run_aggregator import run_aggregator
from publisher.run_publisher import run_publisher


@click.group()
def cli():
    pass


cli.add_command(run_publisher)
cli.add_command(run_aggregator)

if __name__ == "__main__":
    cli()
