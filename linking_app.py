import logging

from flask import Flask, request, jsonify

from customizations.ablation import prepare_ablation
from customizations.extensions import process_input

app = Flask(__name__)

LOG = logging.getLogger("linking_app.py")

question_to_links = {}

@app.route('/')
def home():
    return jsonify({"status": 200})


@app.route('/link', methods=["POST", "GET"])
def linking():
    if request.method == "POST":
        input_text = request.form.get('input_text')
        ablation_flag = request.form.get('ablation') is not None
        approach = request.form.get('approach')
    else:
        input_text = request.args.get('input_text')
        ablation_flag = request.args.get('ablation') is not None
        approach = request.form.get('approach')

    if input_text is None:
        LOG.error("/perform-linking,  Invalid parameters provided")
        return "Invalid parameters provided", 400
    else:
        LOG.info(f"/perform-linking {request.method}, input={input_text}")
        if ablation_flag:
            linking_result = question_to_links[input_text]
            if linking_result is not None:
                return jsonify(linking_result.to_dict())
            else:
                print("ABLATION FLAT SET BUT LINKS NOT FOUND")

        linking_result = process_input(input_text, approach)
        return jsonify(linking_result.to_dict())


if __name__ == '__main__':
    question_to_links = prepare_ablation()
    app.run(host="0.0.0.0", port=9092)
