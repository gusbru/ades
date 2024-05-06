from setuptools import setup, find_packages

setup(
    name="mypackage",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "os",
        "pathlib",
        "json",
        "loguru",
        "requests",
        "yaml",
    ],
)