from setuptools import find_packages, setup

setup(
    name="health_etl_dagster",
    packages=find_packages(exclude=["health_etl_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
