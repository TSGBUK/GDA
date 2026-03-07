from pathlib import Path
import sys

ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Scripts.generic_processors import run_query_menu


if __name__ == "__main__":
    run_query_menu("TransmissionLosses", "Transmission Losses")
