from datetime import datetime
import json
import subprocess
import threading
import time
from flask import Flask, jsonify, request
import psutil
import requests
import yaml
from kubernetes import client, config
from kubernetes.client.rest import ApiException


app = Flask(__name__)

# Event to control the continuous monitoring thread
monitoring_event = threading.Event()
def continuous_monitoring():
    while not monitoring_event.is_set():
        cpu_usage, memory_usage = monitor_resources()
        print(f"Continuous Monitoring - Timestamp: {time.time()}, CPU Usage: {cpu_usage}%, Memory Usage: {memory_usage}%")
        time.sleep(0.01)  

@app.route('/api', methods=['POST'])
def handle_api_request():
    request_data = request.get_json()

    if not request_data:
        return jsonify({'error': 'Invalid request: No data provided'}), 400

    try:
        message_json = request_data['message']
        task = request_data['task']
    except KeyError as e:
        return jsonify({'error': f'Missing key in request data: {e}'}), 400

    try:
        message = json.loads(message_json)
    except json.JSONDecodeError as e:
        return jsonify({'error': f'Invalid JSON in message: {e}'}), 400

    global task_type
    print("Task:", task)

    # Perform task 3
    result = negotiate_cloud()
     
    if result == "success":
        threading.Thread(target=perform_task3, args=(message,task)).start()
        return {'result': 'Task 3 deployed successfully wait for result'}
    else:
        return {'result': 'Task 3 failed because of cloud negotiation failure'}
 

def perform_task3(message,task_type):
    # Monitor initial CPU and memory usage before orchestration
    initial_cpu, initial_memory = monitor_resources()
    print("\ninitial usage")
    print("Timestamp, Human Readable, CPU Usage %, Memory Usage %")
    print(time.time(),',',datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f'),',', initial_cpu,',', initial_memory)

    print("\nStarting orchestration...")
    start_time = time.time()

    deploy_pod()
    deploy_service()

    config.load_kube_config(config_file= "/etc/rancher/k3s/k3s.yaml")

    # Define namespace, Job name, and Service name
    namespace = 'default'
    deployment_name = 'cloud'
    service_name = 'cloud-service'
    flask_ready_log_entry = 'Running on all addresses'
    
    # Check Job, Pod, and Service status
    deployment_ready = False
    service_ready = False
    flask_ready = False

    print("\nTimestamp, Human Readable, CPU Usage %, Memory Usage %")

    while not deployment_ready or not service_ready:
        deployment_ready = check_deployment_status(namespace, deployment_name) and check_pod_status(namespace, deployment_name)
        service_ready = check_service_status(namespace, service_name)

        # Collect CPU and memory usage data during orchestration
        cpu_usage, memory_usage = monitor_resources()
        print(time.time(),',',datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f'),',', cpu_usage,',', memory_usage)

    print("\nDeployment and Service are ready. Checking Flask server status...")
    
    end_time = time.time()
    orchestration_time = end_time - start_time

    flask_time = time.time()

    # Fetch the Pod name
    pod_name = None
    pods = core_v1.list_namespaced_pod(namespace=namespace, label_selector=f"app={deployment_name}")
    if pods.items:
        pod_name = pods.items[0].metadata.name

    print("\nTimestamp, Human Readable, CPU Usage %, Memory Usage %")

    # Check Flask server readiness
    while not flask_ready:
        flask_ready = check_flask_ready(namespace, pod_name, flask_ready_log_entry)
        cpu_usage, memory_usage = monitor_resources()
        print(time.time(),',', datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f'),',', cpu_usage,',', memory_usage)

    print("\nFlask server is ready. Proceeding to send data.")

    end_time = time.time()
    orchestration_and_flask_ready_time = end_time - start_time
    flask_ready_time = end_time - flask_time
    print("\nOrchestration Time:", orchestration_time) 
    print("flask ready time:", flask_ready_time)
    print("Orchestration Time + Flask ready time:", orchestration_and_flask_ready_time)  

    print("Computation Started")
    # Send data to the pod API endpoint
    response = requests.post("http://192.168.1.147:30234/train", json=message)
    print("Computation Ended")
    print(response.text)

    payload = {'message': response.text, 'task': 'task3'}
    # Serialize the payload to JSON
    payload_json = json.dumps(payload)
    # Calculate the size of the JSON payload in bytes
    payload_size = len(payload_json.encode('utf-8'))
    # Print the payload size
    print(f"Payload size: {payload_size} bytes\n")
    
    print("sending data to user")
    before_api = time.time()

    response = requests.post('http://192.168.10.148:5003/api', json=payload)
    after_api = time.time()
    print("API call time:", after_api - before_api)
    # Print the response from the server
    print(response.text)
    

def negotiate_cloud():
    script_path = "/home/admin/github/master-thesis/edge/bash.sh" 
    script_output = run_shell_script(script_path)
    
    # Split the string into lines
    lines = script_output.strip().split('\n')

    # Extract headers
    headers = lines[0].split()

    # Initialize dictionaries to store data for each node
    node_data = {}

    # Process data for each line
    for line in lines[1:]:
        # Split line into fields
        fields = line.split()
        node_name = fields[0]
        node_values = {
            headers[i]: fields[i] for i in range(1, len(headers))
        }
        node_data[node_name] = node_values
    
    negotiation = "unsuccess"
    for node_name, values in node_data.items():
        if int(values['CPU%'].rstrip('%')) < 50 and int(values['MEMORY%'].rstrip('%')) < 50:
            print(f"Node: {node_name} -> CPU usage is {values['CPU%']} and memory usage is {values['MEMORY%']}")
            negotiation = "success"
        else:
            print(f"Node: {node_name} -> CPU usage is {values['CPU%']} and memory usage is {values['MEMORY%']}")

    return negotiation

def run_shell_script(script_path):
    try:
        # Run the shell script using subprocess and capture output
        completed_process = subprocess.run(['bash', script_path], capture_output=True, text=True, check=True)
        # Access the captured output from completed_process.stdout
        script_output = completed_process.stdout
        # Return the captured output
        return script_output
    except subprocess.CalledProcessError as e:
        print(f"Error running shell script: {e}")
        return None

def deploy_pod():
    # Load kubeconfig file to authenticate with the Kubernetes cluster
    config.load_kube_config(config_file= "/etc/rancher/k3s/k3s.yaml")

    # Create Kubernetes API client
    apps_v1 = client.AppsV1Api()

    with open("manifests/deployment.yaml", "r") as file:
        deployment_manifest = yaml.safe_load(file)
        try:
            apps_v1.create_namespaced_deployment(
                body=deployment_manifest, namespace="default"
            )
            print("Deployment created successfully!")
        except Exception as e:
            print(f"Error creating Deployment: {e}")


def deploy_service():
    # Load kubeconfig file to authenticate with the Kubernetes cluster
    config.load_kube_config(config_file="/etc/rancher/k3s/k3s.yaml")

    # Load YAML file containing the service manifest
    with open("manifests/service.yaml", "r") as file:
        service_manifest = yaml.safe_load(file)

    # Create Kubernetes API client
    core_v1 = client.CoreV1Api()

    try:
        # Create the Service
        core_v1.create_namespaced_service(
            body=service_manifest, namespace="default"
        )
        print("Service created successfully!")
    except Exception as e:
        print(f"Error creating Service: {e}")

config.load_kube_config(config_file= "/etc/rancher/k3s/k3s.yaml")
# Initialize API clients
app_v1 = client.AppsV1Api()
core_v1 = client.CoreV1Api()

def check_deployment_status(namespace, deployment_name):
    try:
        deployment = app_v1.read_namespaced_deployment(name=deployment_name, namespace=namespace)
        if deployment.status.ready_replicas == deployment.status.replicas:
            # print("Deployment is ready.")
            return True
        else:
            # print("Deployment is not ready.")
            return False
    except ApiException as e:
        print(f"Exception when reading Deployment: {e}")
        return False
    
def check_pod_status(namespace, deployment_name):
    try:
        pods = core_v1.list_namespaced_pod(namespace=namespace, label_selector=f"app={deployment_name}")
        for pod in pods.items:
            if pod.status.phase == 'Running':
                # print(f"Pod {pod.metadata.name} is running.")
                return True
            
        return False
    except ApiException as e:
        print(f"Exception when listing Pods: {e}")
        return False

def check_service_status(namespace, service_name):
    try:
        service = core_v1.read_namespaced_service(name=service_name, namespace=namespace)
        if service.spec.type == 'LoadBalancer':
            node_port = service.spec.ports[0].node_port
            # print(f"Service is up with NodePort: {node_port}")
            return True
        else:
            print("Service is not of type NodePort.")
            return False
    except ApiException as e:
        print(f"Exception when reading Service: {e}")
        return False

def check_flask_ready(namespace, pod_name, log_entry):
    try:
        logs = core_v1.read_namespaced_pod_log(name=pod_name, namespace=namespace)
        if log_entry in logs:
            # print("Flask server is ready.")
            return True
        else:
            return False
    except ApiException as e:
        print(f"Exception when reading Pod logs: {e}")
        return False

def monitor_resources():
    # Function to capture CPU and memory usage
    cpu_usage = psutil.cpu_percent(interval=0.5)
    memory_info = psutil.virtual_memory()
    return cpu_usage, memory_info.percent



if __name__ == '__main__':
    # monitoring_thread = threading.Thread(target=continuous_monitoring, daemon=True)
    # monitoring_thread.start()
    app.run(host='0.0.0.0', port=5000)