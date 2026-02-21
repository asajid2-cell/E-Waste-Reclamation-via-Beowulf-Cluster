import requests
res = requests.post("http://localhost:5000/api/v1/task", json={"func":"console.log(\"test_print_message\");", "args":[1, 2, 3, 4]})
if res.ok:
    print(res.json())