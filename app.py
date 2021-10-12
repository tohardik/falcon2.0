import logging

from flask import Flask, request, jsonify

from customizations.extensions import process_input

app = Flask(__name__)

LOG = logging.getLogger("app.py")


@app.route('/')
def home():
    return jsonify({"status": 200})


@app.route('/perform-linking', methods=["POST", "GET"])
def linking():
    if request.method == "POST":
        input_text = request.form.get('input_text')
    else:
        input_text = request.args.get('input_text')

    if input_text is None:
        LOG.error("/perform-linking,  Invalid parameters provided")
        return "Invalid parameters provided", 400
    else:
        LOG.info(f"/perform-linking {request.method}, input={input_text}")
        linking_result = process_input(input_text)
        return jsonify(linking_result.to_dict())



if __name__ == '__main__':
    app.run(host="0.0.0.0")
