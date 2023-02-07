
import urllib.request

import pulumi
from pulumi import ResourceOptions
from pulumi_gcp import compute

compute_network = compute.Network(
    "network",
    auto_create_subnetworks=True,
)

my_ip = urllib.request.urlopen("http://wtfismyip.com/text").readlines()[0].decode().strip()

compute_firewall = compute.Firewall(
    "firewall",
    network=compute_network.self_link,
    allows=[compute.FirewallAllowArgs(
        protocol="tcp",
        ports=["22", "80", "3000"],
    )],
    source_ranges=[f"{my_ip}/32"]
)

# A simple bash script that will run when the webserver is initalized
startup_script = """#!/bin/bash
sudo useradd frsght -m -d /home/frsght
git clone https://github.com/ewheeler/foresight /home/frsght/foresight
"""

instance_addr = compute.address.Address("address")
compute_instance = compute.Instance(
    "instance",
    machine_type="n2-highmem-8",
    metadata_startup_script=startup_script,
    boot_disk=compute.InstanceBootDiskArgs(
        initialize_params=compute.InstanceBootDiskInitializeParamsArgs(
            image="deeplearning-platform-release/pytorch-1-13-cpu",
            size=100
        )
    ),
    network_interfaces=[compute.InstanceNetworkInterfaceArgs(
            network=compute_network.id,
            access_configs=[compute.InstanceNetworkInterfaceAccessConfigArgs(
                nat_ip=instance_addr.address
            )],
    )],
    service_account=compute.InstanceServiceAccountArgs(
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    ),
    opts=ResourceOptions(depends_on=[compute_firewall]),
)

pulumi.export("instanceName", compute_instance.name)
pulumi.export("instanceIP", instance_addr.address)
