mpd-cast is a proxy MPD (music player daemon) server for playing music on Chromecast devices. It connects to a real MPD instance for its database/queue backend, and then the playback commands are forwarded to the Chromecast API.

To run an end-to-end test:
  .venv/bin/python -m tests.e2e