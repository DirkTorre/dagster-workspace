- [install postgresql](https://www.postgresql.org/download/)
- first execute setup_postgres.sh
- [install UV](https://docs.astral.sh/uv/getting-started/installation/)
- sync environment with `uv sync`
then activate the environment and run `dg dev`

to use notebooks go to the imdb project folder and run: `uv run python3 -m ipykernel install --user --name=imdb`