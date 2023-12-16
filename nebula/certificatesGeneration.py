import os
import subprocess
import yaml

# List of hosts
hosts = [
    {"name": "fetchingdata-price", "ip": "10.0.0.2/24", "groups": "data,prod"},
    {"name": "fetchingdata-news", "ip": "10.0.0.3/24", "groups": "data,prod"},
    {"name": "socialdata-processing", "ip": "10.0.0.4/24", "groups": "data,prod"},
    {"name": "data-combinator", "ip": "10.0.0.5/24", "groups": "data,prod"},
    {"name": "machinelearning-processing", "ip": "10.0.0.6/24", "groups": "data,prediction,prod"},
    {"name": "LSTM-prevision", "ip": "10.0.0.7/24", "groups": "prediction,prod"},
    {"name": "RF-prevision", "ip": "10.0.0.8/24", "groups": "prediction,prod"},
    {"name": "nebula-lighthouse", "ip": "10.0.0.1/24", "groups": "lighthouse,prod"}
]

cert_dir = "certificates"
config_dir = "configs"
if not os.path.exists(cert_dir):
    os.makedirs(cert_dir)
if not os.path.exists(config_dir):
    os.makedirs(config_dir)

# Base configuration template
base_config = {
    'pki': {
        'ca': '/home/dynamicbtcdca/ca.crt',
        'cert': '',  # Placeholder for certificate path
        'key': '',   # Placeholder for key path
    },
    'static_host_map': {
        "10.0.0.1": ["35.219.178.154:4242"]
    },
    'lighthouse': {
        'am_lighthouse': False,
        'interval': 60,
        'hosts': ["10.0.0.1"]
    },
    'listen': {
        'host': '0.0.0.0',
        'port': 4242
    },
    'punchy': {
        'punch': True
    },
    'relay': {
        'am_relay': False,
        'use_relays': True
    },
    'tun': {
        'disabled': False,
        'dev': 'nebula1',
        'drop_local_broadcast': False,
        'drop_multicast': False,
        'tx_queue': 500,
        'mtu': 1300,
        'routes': [],
        'unsafe_routes': []
    },
    'logging': {
        'level': 'info',
        'format': 'text'
    },
    'firewall': {
        'outbound_action': 'drop',
        'inbound_action': 'drop',
        'conntrack': {
            'tcp_timeout': '1000m',
            'udp_timeout': '1000m',
            'default_timeout': '1000m'
        },
        'outbound': [
            {'port': 22, 'proto': 'tcp', 'group': 'data'}
        ],
        'inbound': [
            {'port': 22, 'proto': 'tcp', 'group': 'data'}
        ]
    }
}

class NoAliasDumper(yaml.Dumper):
    """
    Custom YAML Dumper that does not emit aliases and maintains order.
    """
    def ignore_aliases(self, data):
        return True

    def represent_dict_order(self, data):
        return self.represent_mapping('tag:yaml.org,2002:map', data.items())

# Apply the custom representers
NoAliasDumper.add_representer(dict, NoAliasDumper.represent_dict_order)

# Function to get custom firewall settings for a host
def get_firewall_settings(host_name):
    # Default firewall settings
    firewall_settings = {
        'outbound': [{'port': 22, 'proto': 'tcp', 'group': 'data'}],
        'inbound': [{'port': 22, 'proto': 'tcp', 'group': 'data'}]
    }

    # Customizing firewall settings based on host name
    if host_name == "machinelearning-processing":
        firewall_settings['outbound'] = [{'port': 22, 'proto': 'tcp', 'group': 'prediction'}]
    elif host_name in ["LSTM-prevision", "RF-prevision"]:
        firewall_settings['outbound'] = [{'port': 22, 'proto': 'tcp', 'group': 'prediction'}]
        firewall_settings['inbound'] = [{'port': 22, 'proto': 'tcp', 'group': 'prediction'}]
    elif host_name == "nebula-lighthouse":
        config['lighthouse']['am_lighthouse'] = True
        config['lighthouse']['hosts'] = []
        firewall_settings['outbound'] = [{'port': 22, 'proto': 'tcp', 'group': 'lighthouse'}]
        firewall_settings['inbound'] = [{'port': 22, 'proto': 'tcp', 'group': 'lighthouse'}]

    return firewall_settings

# Generate certs and config files for each host
for host in hosts:
    name = host["name"]
    ip = host["ip"]
    groups = host["groups"]

    # Generate certificate
    subprocess.run(["nebula-cert", "sign", "-name", name, "-ip", ip, "-groups", groups])

    # Copy keys to certificate directory
    subprocess.run(["scp", f"{name}.crt", f"{cert_dir}/{name}.crt"])
    subprocess.run(["scp", f"{name}.key", f"{cert_dir}/{name}.key"])
    subprocess.run(["rm", f"{name}.crt"])
    subprocess.run(["rm", f"{name}.key"])

    # Create config file for this host
    config = base_config.copy()
    config['pki']['cert'] = os.path.join(cert_dir, f"{name}.crt")
    config['pki']['key'] = os.path.join(cert_dir, f"{name}.key")

    # Set custom firewall settings
    firewall_settings = get_firewall_settings(name)
    config['firewall']['outbound'] = firewall_settings['outbound']
    config['firewall']['inbound'] = firewall_settings['inbound']

    # When dumping YAML, use the custom Dumper
    with open(os.path.join(config_dir, f"{name}-config.yml"), 'w') as file:
        yaml.dump(config, file, Dumper=NoAliasDumper, default_flow_style=False)

print("Nebula certs and configs generated!")

