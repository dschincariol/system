from __future__ import annotations

"""Validate the AMD XDNA2 NPU (Ryzen AI) enablement stack on this host.

The XDNA2 NPU on Strix Halo (Ryzen AI Max+ 395) is an INT8 inference accelerator
reached through a different stack than the ROCm GPU:

    kernel driver (amdxdna) + firmware  ->  XRT / xdna userspace  ->
    ONNX Runtime + VitisAI execution provider  ->  INT8-quantized model

This harness is read-only and never loads a model or arms trading. It reports
which layers of that stack are present so the operator knows exactly what work
remains to make the NPU usable, analogous to tools/validate_rocm_acceleration.py
for the GPU. PyTorch does NOT run on the NPU; this is intentionally a separate
contract from the ROCm profile. The probe itself lives in engine.runtime.npu so
the runtime backend resolver and this tool share one source of truth.
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, List

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from engine.runtime.npu import probe_npu_stack  # noqa: E402


def _emit_text(payload: Dict[str, Any]) -> None:
    print("NPU (XDNA2 / Ryzen AI) validation")
    print(f"  kernel_ready    = {payload['kernel_ready']}")
    print(f"  userspace_ready = {payload['userspace_ready']} (XRT / xdna)")
    print(f"  inference_ready = {payload['inference_ready']} (onnxruntime VitisAI EP)")
    print(f"  next_step       = {payload['next_step']}")
    dev = payload["device"]
    print(
        f"  amdxdna_loaded={payload['amdxdna_module_loaded']} "
        f"accel0 exists={dev['exists']} readable={dev['readable']} writable={dev['writable']}"
    )
    fw = payload["firmware"]
    print(f"  firmware dir={fw['dir']} exists={fw['exists']} revisions={fw.get('revisions')}")
    groups = payload["groups"]
    print(f"  groups render={groups.get('render')} video={groups.get('video')}")
    x = payload["xrt"]
    print(f"  xrt-smi={x['xrt_smi']} /opt/xilinx/xrt={x['opt_xilinx_xrt']}")
    o = payload["onnxruntime"]
    print(f"  onnxruntime installed={o['installed']} vitisai={o['vitisai']} providers={o.get('providers')}")


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate AMD XDNA2 NPU enablement stack (read-only).")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument(
        "--require-ready",
        action="store_true",
        help="Exit nonzero unless the full NPU inference stack (XRT + VitisAI EP) is present.",
    )
    args = parser.parse_args(argv)

    payload = probe_npu_stack()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        _emit_text(payload)

    if args.require_ready:
        return 0 if bool(payload.get("inference_ready")) else 1
    return 0 if bool(payload.get("kernel_ready")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
