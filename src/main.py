"""Flask app and entry point."""

from flask import Flask, jsonify, render_template

from .collector import get_status, start_collector

app = Flask(
    __name__,
    static_folder="../public",
    static_url_path="",
    template_folder="../public",
)


@app.after_request
def add_cors(response):
    """Allow cross-origin requests for GitHub Pages frontend."""
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    return jsonify(get_status())


def main():
    start_collector()
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)


if __name__ == "__main__":
    main()
