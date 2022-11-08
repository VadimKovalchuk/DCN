from setuptools import find_packages, setup

setup(
    name='dcn',
    packages=find_packages(
        include=['dcn']
    ),
    install_requires=[
        'pika==1.3.1',
        'pyzmq==24.0.1'
    ],
    version='0.0.1',
    description='Distributed computations network',
    author='Vadym Kovalchuk',
    license='MIT',
)
