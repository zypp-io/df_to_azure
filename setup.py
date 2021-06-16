from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt") as fp:
    install_requires = fp.read()

setup(
    name="df_to_azure",
    version="0.4.0",
    author="Melvin Folkers, Erfan Nariman",
    author_email="melvin@zypp.io, erfan@zypp.io",
    description="Automatically create pipelines with copy activity from blob to SQL",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="python, azure, data-warehouse, sql, blob",
    url="https://github.com/zypp-io/df_to_azure",
    packages=find_packages(),
    install_requires=install_requires,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    project_urls={
        "Bug Reports": "https://github.com/zypp-io/df_to_azure/issues",
        "Source": "https://github.com/zypp-io/df_to_azure",
    },
)
