import time
import pathlib
import argparse
import subprocess

import paramiko

parser = argparse.ArgumentParser(description="set up new VM")
parser.add_argument("username", type=str, default="ewheeler",
                    help="username on VM", nargs='?')
parser.add_argument("pkey", type=str, default="/Users/ewheeler/.ssh/google_compute_engine",
                    help="local path to private key", nargs='?')
#parser.add_argument("ip", type=str, default="34.123.171.183",
#                    help="ip", nargs='?')
args = parser.parse_args()

username = args.username
pkey_path = args.pkey

# find this script's directory
dir_path = pathlib.Path(__file__).resolve().parent

#if args.ip:
#    hostname = args.ip
#else:
if True:
# get IP address of new VM
    _hostname = subprocess.run(["pulumi", "stack", "output", "instanceIP"], capture_output=True)
    if not _hostname.stderr:
        hostname = _hostname.stdout.decode().strip()
    else:
        print(_hostname.stderr)
print(hostname)

pkey = paramiko.RSAKey.from_private_key_file(pkey_path)

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.load_system_host_keys()
c = client.connect(hostname, port=22, username=username, pkey=pkey)

# find location of conda binary
_stdin, _stdout, _stderr = client.exec_command("conda info --base")
if not _stderr:
    conda_base = _stdout.read().decode()
    print(conda_base)
else:
    conda_base = "/opt/conda"
    print(_stderr.read().decode())

# prepare commands
foresight_dir = "/home/frsght/foresight"
conda_source = f"source {conda_base}/etc/profile.d/conda.sh"
conda_activate = f"source {conda_base}/etc/profile.d/conda.sh ; {conda_base}/bin/conda activate ; {conda_base}/bin/conda activate"
conda_lock_file = "conda_platform_locks/conda-linux-64.lock"
conda_create = f"{conda_source} && {conda_base}/bin/conda create --name foresight --file {foresight_dir}/{conda_lock_file} -y"
# eureka! https://plainenglish.io/blog/eureka-i-finally-managed-to-activate-my-conda-environment-in-my-ci-pipeline-a8b578c21f03
poetry_install = f"cd {foresight_dir} && {conda_base}/bin/conda run -n foresight {conda_base}/envs/foresight/bin/poetry install --with prod"

def execute(cmd):
    # pretend to be a login shell to trick conda
    # https://stackoverflow.com/a/72907991
    # TODO this might not be necessary since 
    # discovering `conda run -n env cmd`
    command = f'/bin/bash -lc "{cmd}"'
    stdin, stdout, stderr = client.exec_command(command)
    # stream stdout to terminal
    while True:
        line = stdout.readline()
        if not line:
            break
        print(line, end="")
    if stderr:
        print(stderr.read().decode())
        return None

# NOTE these are idempotent, so no problem if
# rerunning is necessary
cmds = [f"sudo chown -R {username}:{username} /home/frsght/foresight",
        'PS1="${PS1:-}" conda shell.posix activate', # https://github.com/conda/conda/issues/11885#issuecomment-1259508672
        f"source {conda_base}/etc/profile.d/conda.sh ; {conda_base}/bin/conda init bash",
        conda_create,
        poetry_install,
        "sudo apt update -y && sudo apt install python-pip -y", # use system python for supervisor
        "sudo pip install supervisor",
        "sudo systemctl status supervisor"]

for n, cmd in enumerate(cmds):
    print(f"running cmd {n}...")
    execute(cmd)

sftp = client.open_sftp()
print("putting gcp credentials file...")
sftp.put(f"{dir_path.parent}/src/foresight/foresight-375620-01e36cd13a77.json",
         '/home/frsght/foresight/src/foresight/foresight-375620-01e36cd13a77.json')

# these env vars are now specified in supervisord.conf
# so probably not necessary to write this .env file
dotenv = """
# .env

DAGSTER_HOME='/home/frsght/foresight/src/foresight'
DAGSTER_DEPLOYMENT='production'
"""

print("putting .env file...")
with sftp.open('/home/frsght/foresight/src/foresight/.env', 'wb') as f:
    f.write(dotenv)
    f.close()

# user can't write to /etc and sftp can't sudo,
# so put in user's home dir
print("putting supervisord.conf file...")
sftp.put(f"{dir_path}/supervisord.conf",
         f"/home/{username}/supervisord.conf")

if sftp:
    sftp.close()

print("installing GCP Ops agent...")
# enables monitoring of memory and disk via GCP dashboard
execute("curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh")
execute("sudo bash add-google-cloud-ops-agent-repo.sh --also-install")

print("moving supervisord.conf to /etc...")
execute(f"sudo mv /home/{username}/supervisord.conf /etc/supervisord.conf")

print("making log dir...")
execute("sudo mkdir /var/log/supervisord")

print("starting supervisor...")
execute("sudo supervisord -c /etc/supervisord.conf")

print("reloading supervisord.conf...")
execute("sudo supervisorctl reread")
execute("sudo supervisorctl update")

print("starting supervisor programs...")
execute("sudo supervisorctl start all")

print("sleep and check status...")
time.sleep(5)
execute("sudo supervisorctl status")

if client:
    client.close()
