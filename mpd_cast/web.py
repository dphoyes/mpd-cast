import os
import flask
import itertools
from werkzeug.utils import safe_join
from pathlib import Path

app = flask.Flask(__name__)
MUSIC_DIRECTORY = os.path.join(app.root_path, "music_collection")


@app.route("/")
def hello():
    return "Hello World!"


@app.route("/media/<path:file_path>")
def media(file_path):
    return flask.send_from_directory(MUSIC_DIRECTORY, file_path)


@app.route("/albumart/<path:given_path>")
def albumart(given_path):
    given_path = Path(safe_join(MUSIC_DIRECTORY, given_path))
    for parent_dir in itertools.chain((given_path,), given_path.parents):
        if parent_dir == MUSIC_DIRECTORY:
            break
        if parent_dir.is_dir():
            for pattern in ('*.png', '*.jpg', '*.PNG', '*.JPG'):
                for f in parent_dir.glob(pattern):
                    if f.stem.lower() in ('folder', 'cover'):
                        return flask.redirect(
                            flask.url_for('media', file_path=f.relative_to(MUSIC_DIRECTORY)),
                            code=301
                        )
    flask.abort(404)


@app.route("/google-cast-receiver")
def google_cast_receiver():
    return flask.render_template('google-cast.html')


if __name__ == "__main__":
    app.run()
