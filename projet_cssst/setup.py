from setuptools import find_packages, setup

setup(
    name="projet_cssst",
    packages=find_packages(exclude=["projet_cssst_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
