
import os
import json

import htcondor

from flask import Flask
from flask_apscheduler import APScheduler
from flask import Response

import htcondor_autoscale_manager.occupancy_metric
import htcondor_autoscale_manager.patch_annotation

app = Flask(__name__)

config = {}
for key, val in os.environ.items():
    if key.startswith("FLASK_"):
        app.config[key[6:]] = val

scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

g_metric = 1.0
g_metrics = []

@scheduler.task("interval", id="metric_update", seconds=60)
def metric_update():
    oc_string = app.config.get("OCCUPANCY_SETTINGS")
    #print(f"oc_string: {oc_string}")
    if not oc_string:
        print("""FLASK_OCCUPANCY_SETTINGS not set -- example: '[{"NAME": "occupancy_long", "CONSTRAINT": "PARTITION==\"long\"", "POD_LABEL_SELECTOR": """
            """"partition=long", "SCALE_VELOCITY": 1, "IDLE_PODS": 0},{"NAME": "occupancy_gpu","CONSTRAINT": """
            """"PARTITION==\"gpu\"", "POD_LABEL_SELECTOR": "partition=gpu","SCALE_VELOCITY": """
            """1, "IDLE_PODS": 0}]'""")
        return
    occupancy_settings = json.loads(oc_string)
    #print(f"occupancy_settings: {occupancy_settings}")


    #constraints = app.config.get("RESOURCE_CONSTRAINT")
    #if not constraints:
    #    print("RESOURCE_CONSTRAINT not set - cannot compute metric.")
    #    return

    #query = app.config.get("POD_LABEL_SELECTOR")
    #if not query:
    #    print("POD_LABEL_SELECTOR not set - cannot query kubernetes for pods.")
    #    return

    #scale_param = {'velocity': int(app.config.get("SCALE_VELOCITY", 1)),
    #               'idlepods': int(app.config.get("IDLE_PODS", 0))}

    with htcondor.SecMan() as sm:
        if 'BEARER_TOKEN' in app.config:
            sm.setToken(htcondor.Token(app.config['BEARER_TOKEN']))
        elif 'BEARER_TOKEN' in os.environ:
            sm.setToken(htcondor.Token(os.environ['BEARER_TOKEN']))
        elif 'BEARER_TOKEN_FILE' in app.config:
            with open(app.config['BEARER_TOKEN_FILE']) as fp:
                sm.setToken(htcondor.Token(fp.read().strip()))
        elif 'BEARER_TOKEN_FILE' in os.environ:
            with open(os.environ['BEARER_TOKEN_FILE']) as fp:
                sm.setToken(htcondor.Token(fp.read().strip()))
        for ocs in occupancy_settings:
            print(f"query: {ocs['POD_LABEL_SELECTOR']}, constrains: {ocs['CONSTRAINT']}")
            try:
                g_metric, counts = htcondor_autoscale_manager.occupancy_metric(
                        ocs["POD_LABEL_SELECTOR"],
                        ocs["CONSTRAINT"],
                        {'velocity': ocs['SCALE_VELOCITY'],
                         'idlepods': ocs['IDLE_PODS']})
                print(f"myg_metric:{g_metric}")
            except Exception as exc:
                print(f"Exception occurred during metric update: {exc}")
                return
            metric = next((item for item in g_metrics if item['name']==ocs["NAME"]), None)
            if not metric:
                g_metrics.append({"name": ocs["NAME"], "value": g_metric})
            else:
                metric["value"] = g_metric
            # Annotate the 'cost' of deleting the pod.  We only want to patch
            # for changes (which might include when the job originally starts).
            for pod, current_cost in counts['costs'].items():
                desired_cost = 10
                if pod not in counts['online_pods']:
                    desired_cost = 0
                elif pod in counts['idle_pods']:
                    desired_cost = 5
                if desired_cost != current_cost:
                    htcondor_autoscale_manager.patch_annotation(pod, desired_cost)

        #try:
        #    global g_metric
        #    g_metric, counts = htcondor_autoscale_manager.occupancy_metric(query, constraints, scale_param)
        #except Exception as exc:
        #    print(f"Exception occurred during metric update: {exc}")
        #    return


@app.route("/metrics1")
def metrics1():
    return f"occupancy {g_metric}\n"

@app.route("/metrics")
def metrics():
    line = ""
    for m in g_metrics:
        line += f"{m['name']} {m['value']}\n"
    return Response(line, mimetype="text/plain; version=0.0.4; charset=utf-8")

def entry():
    app.run()
