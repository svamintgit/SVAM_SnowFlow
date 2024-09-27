from setuptools import setup, find_packages

setup(
    name="bobsled",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
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
            'bobsled = bobsled:main', 
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)