from setuptools import setup, find_packages

setup(
    name="snowflow",
    version="0.0.15",
    author="Thomas Garcia, Aryan Singh",
    author_email="tgarcia@svam.com, aryan.singh@svam.com",
    description="SnowFlow - A Snowflake Deployment Tool",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    packages=['snowflow'],
    package_dir={'snowflow': 'snowflow'},
    include_package_data=True,
    package_data={'snowflow': ['*.sql', 'snowflow/*.sql']},
    install_requires=[
        "snowflake-connector-python",
        "snowflake-snowpark-python",
        "snowflake-connector-python[secure-local-storage]",
        "networkx",
        "pyyaml",
        "toml",
    ],
    entry_points={
        'console_scripts': [
            'snowflow = snowflow.handler:main',  
            #'snowflow = snowflow:main',
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    license="BSD-3-Clause",
    python_requires='>=3.6',
)