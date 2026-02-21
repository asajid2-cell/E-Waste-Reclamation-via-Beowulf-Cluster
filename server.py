from flask import Flask, request, jsonify
from markupsafe import escape
import subprocess

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

class Job:
	jsFunction = ""
	args = []

def distribute(job):
    docName = "func.txt"
    with open(docName, "w") as text_file:
        text_file.write(job.jsFunction)
    
    with open(docName, "r") as text_file:
        print(text_file.read())

    for arg in job.args:
        subprocess.run(["mujs", docName])

@app.route("/api/v1/task", methods = ["POST"])
def postRequest():
    print(request.json)

    newJob = Job()
    newJob.jsFunction = request.json["func"]
    newJob.args = request.json["args"]

    distribute(newJob)

    return jsonify({"Result":4})

if __name__ == "__main__":
    app.run(debug=True)
