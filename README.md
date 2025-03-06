# Setup Instructions

1. Install `uv`: https://docs.astral.sh/uv/getting-started/installation/
2. Put the voterfile at `./voterfile.parquet`
3. Run the notebook (packages install automatically): `uv run marimo edit match.py`

# Downloading the voterfile

Instructions from Julian:

I have it stashed in a Terrazzo repository- that's the data manipulation project I think I mentioned to you a few months ago. You can see the file here: https://terrazzo.dev/datasets/ny but you'll need to install the Terrazzo client in order to download it. You can download the client from the releases page here: https://gitlab.com/terrazzo-project/terrazzo/-/releases

Once you've downloaded and unpacked the client, you'll want to put the binary (`trzo`) somewhere on your PATH (or just be prepared to invoke it via "./"). If you're on a Mac you'll need to explicitly grant `trzo` permission to run, since it's not signed with a developer key.

Then run these commands:

(1) trzo init  # to initialize your local repository
(2) trzo get ny/elections/voterfile@20240819.0.0  # to download the voterfile dataset to your local repository
(3) trzo dump ny/elections/voterfile@20240819.0.0 --format=parquet > voterfile.parquet  # to export from your repository to a local file

