from setuptools import setup, find_packages

setup(
    name="tsgb-data-ml",
    version="0.1.0",
    description="Machine learning and data processing library for TSGB grid analysis",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    author="TSGB Team",
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=3.9",
    install_requires=[
        "pandas>=1.5.0",
        "pyarrow>=10.0.0",
        "plotly>=5.0.0",
        "numpy>=1.23.0",
        "scikit-learn>=1.2.0",
        "seaborn>=0.12.0",
        "requests>=2.28.0",
    ],
    extras_require={
        "r": ["rpy2>=3.5.0"],
        "gpu": [
            # Note: cudf and dask-cudf should be installed via conda
            # This is here for documentation purposes only
            # "cudf",
            # "dask-cudf",
        ],
        "dev": [
            "jupyter>=1.0.0",
            "jupyterlab>=3.5.0",
            "ipython>=8.0.0",
            "notebook>=6.5.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "tsgb-check-parquet=Scripts.check_parquet:main",
            "tsgb-run-conversions=Scripts.run_parquet_conversions:main",
        ],
    },
    zip_safe=False,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)

