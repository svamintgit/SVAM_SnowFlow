from setuptools import setup, find_packages

setup(
    name="bobsled",
    version="0.1",
    author="Thomas Garcia",
    author_email="tgarcia@svam.com",
    description="Bobsled - A Snowflake Deployment Tool",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "snowflake-connector-python",
        "snowflake-snowpark-python",
        "pyyaml",
        "networkx",
        "toml",
        "argparse",
    ],
    entry_points={
        'console_scripts': [
            'bobsled = bobsled.bobsled:main', 
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