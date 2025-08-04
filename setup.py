from setuptools import setup, find_packages

with open("README.md", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", encoding="utf-8") as fh:
    requirements = fh.read().splitlines()

setup(
    name="gtfstoosm",
    version="0.1.0",
    author="William Hubsch",
    author_email="wahubsch@gmail.com",
    description="Convert GTFS transit feeds to OpenStreetMap relations",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/whubsch/gtfstoosm",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering :: GIS",
    ],
    python_requires=">=3.10",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "gtfstoosm=gtfstoosm.cli:main",
        ],
    },
)
