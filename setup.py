from setuptools import setup, find_packages

setup(
    name='haven',
    version='0.0.1',
    author='Marcel Gietzmann-Sanders',
    author_email='marcelsanders96@gmail.com',
    packages=find_packages(include=['haven', 'haven*']),
    install_requires=[
        'click',
        'aws-cdk-lib',
        'awswrangler',
        'pyyaml',
        'pylint',
    ],
    entry_points={
        'console_scripts': [
            'haven = haven.cli:cli'
        ]
    }
)