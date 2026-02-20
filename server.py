from flask import Flask, request, jsonify
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

@app.route("/api/v1/task", methods = ["POST"])
def postRequest():
    print(request.json)

	# COMPUTE

    return jsonify({"Result":4})

if __name__ == "__main__":
    app.run(debug=True)
