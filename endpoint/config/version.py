import git
import os

APP_VERSION = "0.0.1"
GIT_BRANCH = ""  # git.Repo(os.getcwd()).active_branch.name


def get_version_info():
    return f" {APP_VERSION} {GIT_BRANCH}"
