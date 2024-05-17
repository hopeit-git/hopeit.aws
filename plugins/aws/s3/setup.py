import setuptools

version = "0.1.1"

setuptools.setup(
    name="hopeit.aws.s3",
    version=version,
    description="Hopeit Engine S3 Storage Toolkit",
    license="Apache 2",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Leo Smerling and Pablo Canto",
    author_email="contact@hopeit.com.ar",
    url="https://github.com/hopeit-git/hopeit.aws",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Development Status :: 4 - Beta",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Framework :: AsyncIO",
    ],
    project_urls={
        "CI: GitHub Actions": "https://github.com/hopeit-git/hopeit.aws/actions?query=workflow",
        "GitHub: issues": "https://github.com/hopeit-git/hopeit.aws/issues",
        "GitHub: repo": "https://github.com/hopeit-git/hopeit.aws",
    },
    package_dir={"": "src"},
    packages=[
        "hopeit.aws.s3",
    ],
    include_package_data=True,
    package_data={
        "hopeit.aws.s3": ["py.typed"],
    },
    python_requires=">=3.8",
    extras_require={},
    entry_points={},
)
