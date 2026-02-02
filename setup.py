"""Setup configuration for crdb_to_dbx package."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="crdb_to_dbx",
    version="1.0.0",
    author="Robert Lee",
    author_email="robert.lee@databricks.com",
    description="CockroachDB to Databricks CDC Connector",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rsleedbx/crdb_to_dbx",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.3.0",
        "pg8000>=1.29.0",
        "delta-spark>=2.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=3.0.0",
            "black>=22.0.0",
            "flake8>=4.0.0",
            "mypy>=0.950",
        ],
    },
    entry_points={
        "console_scripts": [],
    },
    include_package_data=True,
    zip_safe=False,
)
