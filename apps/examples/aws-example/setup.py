import setuptools

version = "0.1.0"

setuptools.setup(
    name="aws_example",
    version=version,
    description="Hopeit.py Example App",
    package_dir={"": "src"},
    packages=["aws_example.model", "aws_example.s3"],
    include_package_data=True,
    python_requires=">=3.8",
    install_requires=["hopeit.engine[web,cli]", "hopeit.aws.s3", "aiofiles"],
    extras_require={},
    entry_points={},
)
