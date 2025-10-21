import sys
import os
import subprocess
from src.utils import common as utils

def install_git(users=True):
    proxy = "http://10.144.13.144:3129"
    file_path = ["GitPython==3.1.44", "filelock==3.9.0"]
    if users:
        sys.path.append("/.local/lib/python3.9/site-packages")
        cmd = [sys.executable, "-m", "pip", "install", "--user", "--no-cache-dir", "--proxy", proxy] + file_path
    else: 
        cmd = [sys.executable, "-m", "pip", "install", "--no-cache-dir", "--proxy", proxy] + file_path

    try:
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print("Successfully install packages")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error when install packages", e)
        print(e.stdout)
        print(e.stderr)


def grant_permission(directory):
    subprocess.run(["chmod", "-R", "777", directory], check=True)
    print("Granted permission")


def git_pull_repo(run_mode, local_dir, user_name, password):
    from git import Repo
    from filelock import FileLock

    if run_mode == "dev":
        repo_dir = "http://gitea.apps.cp4d.datalake.vnpt.vn/gitea_admin/code_vnpt.git"
    else:
        repo_dir = "http://gitea.apps.cp4d.datalake.vnpt.vn/gitea_admin/bli_release.git"

    if os.path.exists(local_dir):
        print("pull")
        lock_path = os.path.join(local_dir, "local_dir.lock")
        lock = FileLock(lock_path)

        with lock:
            repo = Repo(local_dir)
            origin = repo.remotes.origin
            https_url = origin.url

            if "@" not in https_url:
                https_url = https_url.replace("http://", f"http://{user_name}:{password}@")

            origin.set_url(https_url)

            # pull code
            origin.fetch()
            repo.git.reset('--hard', 'origin')
            repo.git.merge("origin")
            utils.send_msg_to_telegram(f"Updated, run_mode={run_mode}")

    else:
        print("clone")
        https_url = repo_dir.replace("http://", f"http://{user_name}:{password}@")
        Repo.clone_from(https_url, local_dir)
        utils.send_msg_to_telegram(f"Cloned, run_mode={run_mode}")


### load config
def main():
    os_volume = utils.load_config("config_edit", "to_edit.yaml")["os_volume"]
    common_cfg = utils.load_config("configs", "common.yaml")
    args = utils.process_args_to_dict(sys.argv[1])
    run_mode = args['run_mode']
    directory = f"/code/bli_release"

    # run
    install_git(users=True)
    grant_permission(directory)
    git_pull_repo(run_mode=run_mode,
                local_dir=directory,
                user_name=common_cfg["git"]["usr"],
                password=common_cfg["git"]["pwd"])


if __name__ == "__main__":
    args = utils.process_args_to_dict(sys.argv[1])
    python_file = args['python_file']
    run_mode = args['run_mode']
    spark = utils.create_spark_instance(run_mode=run_mode, python_file=python_file)
    main()