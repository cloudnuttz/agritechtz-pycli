"""Setup module"""

from setuptools import setup, find_packages


with open("README.md", encoding="utf-8") as fd:
    setup(
        name="agritechtz-pycli",
        version="1.0.3",
        description="A client library for fetching and processing crop price data.",
        long_description=fd.read(),
        long_description_content_type="text/markdown",
        author="Moses Kabungo",
        author_email="mose.kabungo@gmail.com",
        url="https://github.com/cloudnuttz/agritechtz.git",
        license="MIT",
        packages=find_packages(exclude=["tests*"]),
        install_requires=[
            "pandas>=1.0.0",
            "requests>=2.0.0",
            "tqdm>=4.0.0",
        ],
        python_requires=">=3.8",
        classifiers=[
            "Development Status :: 4 - Beta",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.11",
            "Topic :: Software Development :: Libraries",
            "Topic :: Scientific/Engineering :: Information Analysis",
        ],
        project_urls={
            "Documentation": "https://github.com/cloudnuttz/agritechtz.git#readme",
            "Source": "https://github.com/cloudnuttz/agritechtz.git",
            "Tracker": "https://github.com/cloudnuttz/agritechtz.git/issues",
        },
    )
