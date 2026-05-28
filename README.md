- [install postgresql](https://www.postgresql.org/download/)
- first execute setup_postgres.sh
- [install UV](https://docs.astral.sh/uv/getting-started/installation/)
- sync environment with `uv sync`
then activate the environment and run `dg dev`

to use notebooks go to the imdb project folder and run: `uv run python3 -m ipykernel install --user --name=imdb`


# google drive api

[The tutorial I used](https://developers.google.com/workspace/drive/api/quickstart/python)

I'm not sure is this setup is correct.

You have to create a project:
[here i think, could be wrong](https://console.cloud.google.com/auth/branding)

Create an app:
[maybe also here?](https://console.cloud.google.com/auth/branding)

Enable the API:

- [link to page](https://console.cloud.google.com/apis/enableflow;apiid=drive.googleapis.com)

Add your gmail accout as a test user:

- [Go to the Google Cloud Console][https://console.cloud.google.com/apis/credentials]
- navigate to "oauth consent screen" > "audience" > "test users" and create an test user with your email


Add the google sheets API to you project:

- [enable the google sheets API](https://console.cloud.google.com/apis/api/sheets.googleapis.com/)