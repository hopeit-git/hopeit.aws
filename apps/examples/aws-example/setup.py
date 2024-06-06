import setuptools

version = "0.1.3"

setuptools.setup(
    name="aws_example",
    version=version,
    description="hopeit.aws plugins Example App",
    package_dir={"": "src"},
    packages=["aws_example.model", "aws_example.s3"],
    include_package_data=True,
    python_requires=">=3.8",
    extras_require={},
    entry_points={},
)
