import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="krbtools",
    version="0.0.1",
    author="Mr.xs",
    author_email="xiashuo@sinosoft.com.cn",
    description="A tools for kerberos authentication.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Nishuoguoguo/krb-tools",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)