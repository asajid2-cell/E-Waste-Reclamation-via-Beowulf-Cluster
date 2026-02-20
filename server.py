from flask import Flask, request
from markupsafe import escape

app = Flask(__name__)

@app.route("/")
def server_response():
    code = request.args.get("code", "")
    return f"""
    <html>
      <body>
        <h1>Received JavaScript Code</h1>
        <pre>{escape(code)}</pre>
      </body>
    </html>
    """

if __name__ == "__main__":
    app.run(debug=True)
