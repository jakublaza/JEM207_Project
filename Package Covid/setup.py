import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="IES_Project_Covid10",
    version="1",
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
    packages=["app"],
    package_dir={'app':'app'},
    package_data={"app": ['data/datacovid.bz2', "data/map/*", "docs/_build/*", "requirements.txt"]}, 
    python_requires=">=3.6",
)

