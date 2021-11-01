import setuptools
    
with open("README.md", "r") as fh:
    long_description = fh.read()
    
setuptools.setup(
    name='keepvariable',
    version='0.1.8',
    author='DovaX',
    author_email='dovax.ai@gmail.com',
    description='A Python package keeping the values of variables between separate runs in a seamless and effortless way.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/DovaX/keepvariable',
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
          ''
     ],
    python_requires='>=3.6',
)
    