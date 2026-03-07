#!/usr/bin/env python3
"""Verification script to check TSGB environment setup.

This script tests all critical dependencies including CPU packages,
GPU/RAPIDS libraries, and system resources. Run after completing
the setup.sh installation to ensure everything is working properly.
"""

import sys
import subprocess
from importlib import import_module
from importlib import metadata


def check_package(name, display_name=None, optional=False):
    """Try importing a package and report status."""
    display = display_name or name
    try:
        mod = import_module(name)
        version = getattr(mod, "__version__", "unknown")
        if version == "unknown":
            try:
                version = metadata.version(name)
            except metadata.PackageNotFoundError:
                pass
        print(f"   {display:20s} version {version}")
        return True
    except ImportError:
        status = "optional, not installed" if optional else "MISSING"
        print(f"   {display:20s} {status}")
        return optional


def check_gpu():
    """Check NVIDIA GPU availability."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,driver_version,memory.total",
             "--format=csv,noheader"],
            capture_output=True,
            text=True,
            check=True,
        )
        gpus = result.stdout.strip().split("\n")
        print(f"   GPU(s) detected ({len(gpus)}):")
        for gpu in gpus:
            print(f"    - {gpu}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("   No NVIDIA GPU detected or nvidia-smi not available")
        return False


def check_cuda():
    """Check CUDA toolkit installation."""
    try:
        result = subprocess.run(
            ["nvcc", "--version"],
            capture_output=True,
            text=True,
            check=True,
        )
        # Extract version from output
        for line in result.stdout.split("\n"):
            if "release" in line.lower():
                print(f"   CUDA toolkit: {line.strip()}")
                return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("   CUDA toolkit compiler (nvcc) not found")
        return False


def main():
    print("=" * 60)
    print("TSGB Environment Verification")
    print("=" * 60)
    
    # Core dependencies
    print("\n[1] Core Python Packages:")
    all_ok = True
    all_ok &= check_package("pandas", "Pandas")
    all_ok &= check_package("numpy", "NumPy")
    all_ok &= check_package("pyarrow", "PyArrow")
    all_ok &= check_package("plotly", "Plotly")
    all_ok &= check_package("sklearn", "scikit-learn")
    all_ok &= check_package("seaborn", "Seaborn")
    all_ok &= check_package("requests", "Requests")
    
    # Optional packages
    print("\n[2] Optional Packages:")
    check_package("rpy2", "rpy2 (R integration)", optional=True)
    check_package("jupyter", "Jupyter", optional=True)
    check_package("IPython", "IPython", optional=True)
    check_package("matplotlib", "Matplotlib", optional=True)
    
    # GPU/RAPIDS
    print("\n[3] GPU & RAPIDS Libraries:")
    gpu_ok = check_gpu()
    cuda_ok = check_cuda()
    
    cudf_ok = check_package("cudf", "cuDF", optional=True)
    cuml_ok = check_package("cuml", "cuML", optional=True)
    dask_cudf_ok = check_package("dask_cudf", "Dask-cuDF", optional=True)

    rapids_any = cudf_ok or cuml_ok or dask_cudf_ok
    rapids_core_ok = cudf_ok and dask_cudf_ok
    
    # Python version
    print("\n[4] Python Environment:")
    py_version = sys.version.split()[0]
    print(f"   Python version: {py_version}")
    print(f"   Executable: {sys.executable}")
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary:")
    if all_ok:
        print("   All core dependencies installed correctly")
    else:
        print("   Some core dependencies are missing - check errors above")
    
    if gpu_ok and rapids_core_ok:
        if cuda_ok:
            print("   GPU acceleration configured (CUDA toolkit + RAPIDS)")
        else:
            print("   GPU acceleration configured (RAPIDS runtime; nvcc toolkit not detected)")
            print("   Note: nvcc is optional unless you compile CUDA code")
    elif gpu_ok and rapids_any:
        print("   GPU detected with partial RAPIDS install")
    elif gpu_ok:
        print("   GPU detected but RAPIDS not installed")
    elif rapids_any:
        print("   RAPIDS packages detected, but no NVIDIA GPU detected")
    else:
        print("   CPU-only mode (GPU/RAPIDS not available)")
    
    print("=" * 60)
    
    # Test a simple operation
    print("\n[5] Quick Functionality Test:")
    try:
        import pandas as pd
        import numpy as np
        
        df = pd.DataFrame({"a": np.random.rand(100), "b": np.random.rand(100)})
        result = df.describe()
        print(f"   Pandas computation successful (shape: {result.shape})")
        
        # Try GPU if available
        try:
            import cudf
            gdf = cudf.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
            print(f"   cuDF computation successful (shape: {gdf.shape})")
        except Exception:
            pass
        
        print("\n Environment is ready for TSGB data processing!")
        return 0
        
    except Exception as e:
        print(f"   Test failed: {e}")
        print("\n Environment has issues - review errors above")
        return 1


if __name__ == "__main__":
    sys.exit(main())
