import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ies_project-kuba4",
    version="0.0.4",
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
    include_package_data=True,
    package_data={"my_package": ['data/*']},
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
)