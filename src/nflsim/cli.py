from typer import Typer

app = Typer(help="nflsim CLI (skeleton). Fill in pipelines before use.")


@app.command()
def hello():
    """Sanity check command."""
    print("nflsim skeleton is installed. Fill in the pipelines to proceed.")
