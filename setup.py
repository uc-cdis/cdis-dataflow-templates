from __future__ import absolute_import
from __future__ import print_function

import setuptools

REQUIRED_PACKAGES = [
    "google-cloud-storage==1.28.0",
    "google-cloud-pubsub==1.5.0",
    "setuptools>=40.3.0",
]

PACKAGE_NAME = "bucket_manifest"
PACKAGE_VERSION = "0.0.1"
setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description="required dependencies",
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)
