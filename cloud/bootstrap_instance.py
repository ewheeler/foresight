import subprocess

import paramiko

_hostname = subprocess.run(["pulumi", "stack", "output", "instanceIP"], capture_output=True)
if not _hostname.stderr:
    hostname = _hostname.stdout.decode().strip()
print(hostname)

# TODO take cli args
username = "ewheeler"
pkey = paramiko.RSAKey.from_private_key_file("/Users/ewheeler/.ssh/google_compute_engine")

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.load_system_host_keys()
c = client.connect(hostname, port=22, username=username, pkey=pkey)

_stdin, _stdout, _stderr = client.exec_command("conda info --base")
if not _stderr:
    conda_base = _stdout.read().decode()
    print(conda_base)
else:
    conda_base = "/opt/conda"
    print(_stderr.read().decode())

foresight_dir = "/home/frsght/foresight"
conda_source = f"source {conda_base}/etc/profile.d/conda.sh"
conda_activate = f"source {conda_base}/etc/profile.d/conda.sh ; {conda_base}/bin/conda activate ; {conda_base}/bin/conda activate"
conda_lock_file = "conda_platform_locks/conda-linux-64.lock"
conda_create = f"{conda_source} && {conda_base}/bin/conda create --name foresight --file {foresight_dir}/{conda_lock_file} -y"
# eureka! https://plainenglish.io/blog/eureka-i-finally-managed-to-activate-my-conda-environment-in-my-ci-pipeline-a8b578c21f03
poetry_install = f"cd {foresight_dir} && {conda_base}/bin/conda run -n foresight {conda_base}/envs/foresight/bin/poetry install"

def execute(cmd):
    # pretend to be a login shell
    command = f'/bin/bash -lc "{cmd}"'
    stdin, stdout, stderr = client.exec_command(command)
    while True:
        line = stdout.readline()
        if not line:
            break
        print(line, end="")
    if stderr:
        print(stderr.read().decode())
        return None

cmds = ["sudo chown -R ewheeler:ewheeler /home/frsght/foresight",
        'PS1="${PS1:-}" conda shell.posix activate', # https://github.com/conda/conda/issues/11885#issuecomment-1259508672
        f"source {conda_base}/etc/profile.d/conda.sh ; {conda_base}/bin/conda init bash",
        conda_create,
        poetry_install]

for n, cmd in enumerate(cmds):
    print(f"running cmd {n}...")
    execute(cmd)

sftp = client.open_sftp()
print("putting file...")
sftp.put('/Users/ewheeler/dev/foresight/src/foresight/foresight-375620-01e36cd13a77.json',
         '/home/frsght/foresight/src/foresight/foresight-375620-01e36cd13a77.json')
if sftp:
    sftp.close()

if client:
    client.close()
