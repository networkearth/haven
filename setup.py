from setuptools import setup, find_packages

setup(
    name='haven',
    version='0.0.1',
    author='Marcel Gietzmann-Sanders',
    author_email='marcelsanders96@gmail.com',
    packages=find_packages(include=['haven', 'haven*']),
    install_requires=[
        'click==8.1.7',
        'aws-cdk-lib==2.154.1',
        'awswrangler==3.9.1',
        'pyyaml==6.0.2',
    ],
    entry_points={
        'console_scripts': [
            'haven = haven.cli:cli'
        ]
    }
)