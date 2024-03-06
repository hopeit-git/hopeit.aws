import setuptools

version = "0.1.0"

setuptools.setup(
    name="aws_example",
    version=version,
    description="Hopeit.py Example App",
    package_dir={"": "src"},
    packages=["common", "model", "aws_example"],
    include_package_data=True,
    python_requires=">=3.8",
    install_requires=[
        "hopeit.engine[web,cli,redis-streams,fs-storage]",
        "hopeit.aws.s3",
        "aiofiles",
    ],
    extras_require={},
    entry_points={},
)
