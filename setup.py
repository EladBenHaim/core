from setuptools import setup

setup(
    name='buckets3',
    version='0.1.0',    
    description='A example Python package',
    url='https://github.com/od-machine/machine-db/tree/main/app/core/buckets3',
    author='ODM',
    packages=[],
    install_requires=['pandas==1.2.2',
                      'boto3==1.17.39',
                      's3fs==2021.11.0',                  
                      ],
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 3.8',
    ],
)
import setuptools

setuptools.setup(
    name="bucketS3",
    version="0.0.1",
    author="ODM",
    description="Read and write files to aws s3",
    url="https://github.com/pypa/sampleproject",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)