from setuptools import setup, find_packages

setup(
    name="ades",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "boto3~=1.34.99",
        "requests~=2.31.0",
        "PyYAML~=6.0.1",
        "loguru~=0.7.2",
        "pystac~=1.10.1",
        "kubernetes~=29.0.0"
    ],
)