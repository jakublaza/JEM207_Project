import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="Project Python IES",
    version="0.0.2",
    author="Jakub Láža",
    author_email="jakub.laza23@gmail.com",
    description="A small package for IES JEM 207 Project",
    license = "LICENSE.txt",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jakublaza/JEM207_Project",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)