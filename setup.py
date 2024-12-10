from setuptools import setup, find_packages

setup(
    name='dikw-accelerator',
    version='0.1.13',
    packages=find_packages(where='lib'),
    package_dir={'': 'lib'},
    install_requires=[
        "icecream",
        "loguru"
    ],
)
