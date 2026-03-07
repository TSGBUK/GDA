#!/bin/bash
# Setup script for Ubuntu with CUDA support (run as root or with sudo)
# Installs system packages, conda environment, and all Python dependencies for
# TSGB data processing and machine learning workflows.
#
# Prerequisites:
#   - Ubuntu 20.04+ with CUDA toolkit installed (tested with CUDA 11.8+)
#   - NVIDIA drivers properly configured
#   - Internet connection for package downloads

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONDA_ENV_NAME="tsgb_rapids"
CONDA_INSTALL_DIR="/opt/miniconda3"
RAPIDS_INSTALLED=0

if [ "${EUID:-$(id -u)}" -eq 0 ]; then
    SUDO=""
else
    if command -v sudo >/dev/null 2>&1; then
        SUDO="sudo"
    else
        echo "ERROR: This script needs root privileges for system package installation."
        echo "Run as root or install sudo and re-run."
        exit 1
    fi
fi

if [ -n "$SUDO" ] && [ ! -w "/opt" ]; then
    CONDA_INSTALL_DIR="$HOME/miniconda3"
fi

echo "================================================"
echo "TSGB Data Processing & ML Environment Setup"
echo "================================================"

# 1. Update system and install base dependencies
echo ""
echo "[1/6] Updating apt repositories and installing system packages..."
$SUDO apt-get update
$SUDO apt-get install -y \
    python3 python3-dev python3-pip \
    build-essential gcc g++ gfortran \
    git wget curl unzip \
    libssl-dev libffi-dev \
    libhdf5-dev libnetcdf-dev \
    libxml2-dev libxslt1-dev \
    libblas-dev liblapack-dev \
    zlib1g-dev

# R dependencies (for rpy2 if needed)
echo "Installing R and dependencies for rpy2..."
$SUDO apt-get install -y r-base r-base-dev

# Node.js (optional; for any npm-based tooling)
$SUDO apt-get install -y nodejs npm

# 2. Verify CUDA installation
echo ""
echo "[2/6] Verifying CUDA installation..."
if command -v nvcc &> /dev/null; then
    CUDA_VERSION=$(nvcc --version | grep "release" | awk '{print $5}' | cut -d',' -f1)
    echo "✓ CUDA detected: version $CUDA_VERSION"
else
    echo "⚠ WARNING: nvcc not found. Please ensure CUDA toolkit is installed."
    echo "  Visit: https://developer.nvidia.com/cuda-downloads"
fi

if command -v nvidia-smi &> /dev/null; then
    echo "✓ NVIDIA driver detected:"
    nvidia-smi --query-gpu=name,driver_version --format=csv,noheader
else
    echo "⚠ WARNING: nvidia-smi not found. GPU may not be available."
fi

# 3. Install Miniconda (if not already present)
echo ""
echo "[3/6] Installing Miniconda..."
if [ ! -d "$CONDA_INSTALL_DIR" ]; then
    cd /tmp
    wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
    if [ -n "$SUDO" ] && [ "$CONDA_INSTALL_DIR" = "/opt/miniconda3" ]; then
        $SUDO bash miniconda.sh -b -p "$CONDA_INSTALL_DIR"
    else
        bash miniconda.sh -b -p "$CONDA_INSTALL_DIR"
    fi
    rm miniconda.sh
    echo "✓ Miniconda installed to $CONDA_INSTALL_DIR"
else
    echo "✓ Miniconda already installed at $CONDA_INSTALL_DIR"
fi

# Initialize conda for bash
export PATH="$CONDA_INSTALL_DIR/bin:$PATH"
eval "$($CONDA_INSTALL_DIR/bin/conda shell.bash hook)"

# Accept conda Terms of Service for unattended runs on newer conda versions
if conda tos --help >/dev/null 2>&1; then
    conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main || true
    conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r || true
fi

# 4. Create conda environment with RAPIDS and ML packages
echo ""
echo "[4/6] Creating conda environment '$CONDA_ENV_NAME' with RAPIDS..."
if conda env list | grep -q "^${CONDA_ENV_NAME} "; then
    echo "Environment '$CONDA_ENV_NAME' already exists. Removing and recreating..."
    conda env remove -n "$CONDA_ENV_NAME" -y
fi

# Try to create environment with Python 3.11 and RAPIDS first.
# If RAPIDS install fails (common on unsupported CUDA/Python combos),
# fall back to a CPU-only environment so setup can still complete.
RAPIDS_SPECS=(python=3.11 cudf dask-cudf cuml cugraph cuda-version=11.8)
set +e
conda create -n "$CONDA_ENV_NAME" -y \
    -c rapidsai -c nvidia -c conda-forge \
    "${RAPIDS_SPECS[@]}"
RAPIDS_CREATE_EXIT=$?

if [ $RAPIDS_CREATE_EXIT -ne 0 ]; then
    conda create -n "$CONDA_ENV_NAME" -y \
        -c rapidsai -c nvidia -c conda-forge \
        -- "${RAPIDS_SPECS[@]}"
    RAPIDS_CREATE_EXIT=$?
fi
set -e

if [ $RAPIDS_CREATE_EXIT -eq 0 ]; then
    RAPIDS_INSTALLED=1
    echo "✓ RAPIDS environment created"
else
    echo "⚠ RAPIDS install failed (likely CUDA/version/channel compatibility)."
    echo "  Falling back to CPU-only conda environment."
    conda create -n "$CONDA_ENV_NAME" -y python=3.11
fi

# Activate the environment
conda activate "$CONDA_ENV_NAME"

# Install CUDA compiler tooling in the conda env when GPU stack is enabled.
# This ensures nvcc is available for verification scripts that check toolkit presence.
if [ $RAPIDS_INSTALLED -eq 1 ]; then
    if command -v nvidia-smi >/dev/null 2>&1; then
        if ! command -v nvcc >/dev/null 2>&1; then
            echo "Installing CUDA compiler tools (nvcc) in '$CONDA_ENV_NAME'..."
            set +e
            conda install -y -c nvidia cuda-nvcc
            NVCC_INSTALL_EXIT=$?
            set -e
            if [ $NVCC_INSTALL_EXIT -eq 0 ]; then
                echo "✓ CUDA compiler tools installed"
            else
                echo "⚠ Could not install cuda-nvcc. RAPIDS runtime may still work without nvcc."
            fi
        fi
    fi
fi

# 5. Install additional Python packages from requirements.txt
echo ""
echo "[5/6] Installing Python packages from requirements.txt..."
if [ -f "$SCRIPT_DIR/requirements.txt" ]; then
    # When RAPIDS is installed, keep conda-provided pyarrow compatible with cudf
    # (cudf 25.x requires pyarrow < 20).
    if [ $RAPIDS_INSTALLED -eq 1 ]; then
        FILTERED_REQS="$(mktemp)"
        grep -Eiv '^pyarrow([[:space:]]|[<>=!~]|$)' "$SCRIPT_DIR/requirements.txt" > "$FILTERED_REQS"
        pip install --no-cache-dir -r "$FILTERED_REQS"
        rm -f "$FILTERED_REQS"
    else
        # CPU-only path can use full requirements as-pinned.
        pip install --no-cache-dir -r "$SCRIPT_DIR/requirements.txt"
    fi
    
    echo "✓ Python packages installed"
else
    echo "⚠ requirements.txt not found at $SCRIPT_DIR/requirements.txt"
fi

# Install additional ML/scientific packages
echo "Installing additional ML dependencies..."
conda install -y -c conda-forge \
    scikit-learn=1.7.2 matplotlib seaborn \
    jupyter jupyterlab ipython \
    notebook

if [ $RAPIDS_INSTALLED -eq 0 ]; then
    echo ""
    echo "ℹ Skipping RAPIDS package verification because CPU fallback was used."
    echo "  You can retry GPU packages later with:"
    echo "  conda install -n $CONDA_ENV_NAME -c rapidsai -c nvidia -c conda-forge cudf dask-cudf cuml cugraph cuda-version=11.8"
fi

# 6. Final setup and verification
echo ""
echo "[6/6] Finalizing setup..."
cd "$SCRIPT_DIR"

# Create activation helper script
cat > "$SCRIPT_DIR/activate_env.sh" << EOF
#!/bin/bash
# Activate the TSGB RAPIDS environment
export PATH="$CONDA_INSTALL_DIR/bin:\$PATH"
eval "\$($CONDA_INSTALL_DIR/bin/conda shell.bash hook)"
conda activate $CONDA_ENV_NAME
echo "✓ TSGB environment activated"
echo "Python: \$(which python)"
echo "Available GPUs:"
python -c "import cudf; print(cudf.__version__)" 2>/dev/null && echo "  - cuDF available" || echo "  - cuDF not available"
EOF

chmod +x "$SCRIPT_DIR/activate_env.sh"

echo ""
echo "================================================"
echo "Setup Complete!"
echo "================================================"
echo ""
echo "To activate the environment, run:"
echo "  source $SCRIPT_DIR/activate_env.sh"
echo ""
echo "Or manually:"
echo "  source $CONDA_INSTALL_DIR/bin/activate"
echo "  conda activate $CONDA_ENV_NAME"
echo ""
echo "Installed components:"
echo "  ✓ Python $(python --version 2>&1 | awk '{print $2}')"
if [ $RAPIDS_INSTALLED -eq 1 ]; then
    echo "  ✓ RAPIDS (cuDF, cuML, Dask-cuDF)"
else
    echo "  ⚠ RAPIDS not installed (CPU fallback mode)"
fi
echo "  ✓ PyArrow, Pandas, NumPy"
echo "  ✓ Plotly, Seaborn, Matplotlib"
echo "  ✓ scikit-learn, Jupyter"
echo "  ℹ R integration (rpy2) is optional and installed separately if needed"
echo ""
echo "Next steps:"
echo "  1. Activate environment: source $SCRIPT_DIR/activate_env.sh"
echo "  2. Verify installation: python Scripts/verify_setup.py"
echo "  3. Convert data to parquet: python Scripts/run_parquet_conversions.py . --run"
echo "  4. Launch Jupyter: jupyter lab"
echo ""
