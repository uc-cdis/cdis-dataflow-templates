from __future__ import absolute_import
from __future__ import print_function

import setuptools
from subprocess import check_output


def get_version():
    try:
        tag = check_output(
            ["git", "describe", "--tags", "--abbrev=0", "--match=[0-9]*"]
        )
        return tag.decode("utf-8").strip("\n")
    except Exception:
        raise RuntimeError(
            "The version number cannot be extracted from git tag in this source "
            "distribution; please either download the source from PyPI, or check out "
            "from GitHub and make sure that the git CLI is available."
        )


REQUIRED_PACKAGES = [
    "google-cloud-storage==1.28.0",
    "google-cloud-pubsub==1.5.0",
    "setuptools>=40.3.0",
]

PACKAGE_NAME = "bucket_manifest"
PACKAGE_VERSION = get_version()
setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description="required dependencies",
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)
