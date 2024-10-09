from setuptools import setup, find_packages

setup(
    name='cbs',
    version='0.3',
    packages=find_packages(where='lib'),
    package_dir={'': 'lib'},
    install_requires=[
    ],
)