import os
import subprocess

# List of hosts
hosts = [
    {"name": "nebula-lighthouse", "ip": "10.0.0.1/24", "groups": "lighthouse,prod"},
    {"name": "fetchingdata-price", "ip": "10.0.0.2/24", "groups": "data,prod"},
    {"name": "fetchingdata-news", "ip": "10.0.0.3/24", "groups": "data,prod"},
    {"name": "socialdata-processing", "ip": "10.0.0.4/24", "groups": "data,prod"},
    {"name": "data-combinator", "ip": "10.0.0.5/24", "groups": "data,prod"},
    {"name": "machinelearning-processing", "ip": "10.0.0.6/24", "groups": "data,prediction,prod"},
    {"name": "LSTM-prevision", "ip": "10.0.0.7/24", "groups": "prediction,prod"},
    {"name": "RF-prevision", "ip": "10.0.0.8/24", "groups": "prediction,prod"},
]

cert_dir = "certificates"
if not os.path.exists(cert_dir):
    os.makedirs(cert_dir)

# Generate certs for each host
for host in hosts:
    name = host["name"]
    ip = host["ip"] 
    groups = host["groups"]
    
    subprocess.run(["nebula-cert", "sign", "-name", name, "-ip", ip, "-groups", groups])
    
    # Copy keys to certificate directory
    subprocess.run(["scp", f"{name}.crt", f"{cert_dir}/{name}.crt"])
    subprocess.run(["scp", f"{name}.key", f"{cert_dir}/{name}.key"])
    #delete cert in currents script dir
    subprocess.run(["rm", f"{name}.crt"])
    subprocess.run(["rm", f"{name}.key"])

print("Nebula certs generated and saved to certificates directory!")
