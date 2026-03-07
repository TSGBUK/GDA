#!/bin/bash
# Activate the TSGB RAPIDS environment
export PATH="/home/ubuntu/miniconda3/bin:$PATH"
eval "$(/home/ubuntu/miniconda3/bin/conda shell.bash hook)"
conda activate tsgb_rapids
echo "✓ TSGB environment activated"
echo "Python: $(which python)"
echo "Available GPUs:"
python -c "import cudf; print(cudf.__version__)" 2>/dev/null && echo "  - cuDF available" || echo "  - cuDF not available"
