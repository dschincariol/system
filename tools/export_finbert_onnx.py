from __future__ import annotations

"""Export FinBERT to ONNX and INT8-quantize it for the XDNA2 NPU.

This is the model-compilation step of the NPU enablement path: the NPU runs
INT8-quantized ONNX graphs through ONNX Runtime's VitisAI execution provider, so
the torch FinBERT checkpoint must first be exported to ONNX and quantized.

Offline tooling only: no trading, no broker, no secrets. It downloads/loads a
public HF model and writes artifacts to a local directory.

Pipeline:
    HF FinBERT  --(optimum-onnx)-->  model.onnx  --(quantize)-->  model.int8.onnx

Quantization modes (`--quant`):
  static  (default, NPU-optimal) -- quantizes weights AND activations using a
          calibration set, which is what the VitisAI/NPU compiler needs for good
          op coverage. Prefers AMD's `vai_q_onnx` (Quark) when installed, else
          falls back to onnxruntime.quantization.quantize_static (QDQ).
  dynamic -- weights only; quickest, runs on CPU EP, weaker NPU coverage.
  none    -- export FP32 ONNX only.

Use `--calibration-file` (one text sample per line) for representative data;
otherwise a built-in financial-text calibration set is used. Use `--from-onnx`
to (re)quantize an existing FP32 ONNX without re-exporting (handy because export
needs torch>=2.6 while static quantization does not).

Point the runtime at the result: FINBERT_ONNX_PATH=<out>/model.int8.onnx and
FINBERT_BACKEND=onnx-vitisai. Verify the NPU stack with
tools/validate_npu_acceleration.py.
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Representative financial text spanning positive / negative / neutral and a
# range of lengths, so static calibration sees realistic activation ranges.
DEFAULT_CALIBRATION_TEXTS = [
    "Quarterly earnings beat analyst expectations as margins expanded sharply.",
    "The company raised full-year guidance on strong demand and order backlog.",
    "Shares surged after the firm announced a buyback and a dividend increase.",
    "Record free cash flow let the board accelerate debt reduction this quarter.",
    "Revenue grew double digits with broad strength across every region.",
    "The acquisition is expected to be accretive to earnings within a year.",
    "Shares slumped after the company cut full-year guidance on weak orders.",
    "The firm missed estimates and warned of softening demand into next quarter.",
    "Default risk rose as the issuer missed a coupon payment on senior notes.",
    "Management slashed the dividend and suspended buybacks to preserve cash.",
    "A profit warning and widening losses sent the stock to a 52-week low.",
    "Regulators opened an antitrust probe into the proposed acquisition.",
    "An accounting restatement raised concerns about internal controls.",
    "The central bank held rates steady, citing balanced risks to the outlook.",
    "The company reported results broadly in line with consensus expectations.",
    "Trading volumes were muted ahead of the holiday-shortened week.",
    "The board reaffirmed prior guidance and made no change to its outlook.",
    "Management said it continues to monitor macroeconomic conditions.",
    "Crude prices were little changed as supply and demand stayed balanced.",
    "The filing disclosed routine related-party transactions for the period.",
    "Inflation cooled modestly while the labor market remained resilient.",
    "Analysts left their price targets unchanged after the in-line print.",
    "The merger received regulatory clearance with no remedies required.",
    "Bond yields edged higher after a solid but unremarkable jobs report.",
]


def _read_calibration_texts(path: Optional[str]) -> List[str]:
    if not path:
        return list(DEFAULT_CALIBRATION_TEXTS)
    with open(path, "r", encoding="utf-8") as handle:
        texts = [line.strip() for line in handle if line.strip()]
    if not texts:
        raise SystemExit(f"calibration_file_empty:{path}")
    return texts


def _export_onnx(model_name: str, out_dir: str) -> str:
    try:
        from optimum.onnxruntime import ORTModelForSequenceClassification  # type: ignore
        from transformers import AutoTokenizer  # type: ignore
    except Exception as exc:
        raise SystemExit(
            "export_deps_missing: install the NPU profile first "
            "(pip install -r requirements-amd-npu.txt) in an env with torch>=2.6 "
            "(the ROCm container qualifies). "
            f"detail={type(exc).__name__}: {exc}"
        )
    os.makedirs(out_dir, exist_ok=True)
    model = ORTModelForSequenceClassification.from_pretrained(model_name, export=True)
    model.save_pretrained(out_dir)
    AutoTokenizer.from_pretrained(model_name).save_pretrained(out_dir)
    onnx_path = os.path.join(out_dir, "model.onnx")
    if not os.path.exists(onnx_path):
        candidates = [f for f in os.listdir(out_dir) if f.endswith(".onnx")]
        if not candidates:
            raise SystemExit(f"onnx_export_produced_no_file:{out_dir}")
        onnx_path = os.path.join(out_dir, candidates[0])
    return onnx_path


def _load_tokenizer(model_name: str, onnx_path: str) -> Any:
    """Tokenizer for calibration: prefer the one saved next to the model."""
    from transformers import AutoTokenizer  # type: ignore

    local_dir = os.path.dirname(onnx_path)
    if os.path.exists(os.path.join(local_dir, "tokenizer_config.json")):
        return AutoTokenizer.from_pretrained(local_dir)
    return AutoTokenizer.from_pretrained(model_name)


def _model_input_names(onnx_path: str) -> List[str]:
    import onnxruntime as ort  # type: ignore

    session = ort.InferenceSession(onnx_path, providers=["CPUExecutionProvider"])
    return [i.name for i in session.get_inputs()]


def _build_calibration_reader(onnx_path: str, tokenizer: Any, texts: List[str], max_len: int):
    from onnxruntime.quantization import CalibrationDataReader  # type: ignore
    import numpy as np

    input_names = set(_model_input_names(onnx_path))

    class _FinbertCalibrationReader(CalibrationDataReader):
        def __init__(self) -> None:
            self._iter = None

        def _build(self):
            samples = []
            for text in texts:
                enc = tokenizer(
                    [text],
                    return_tensors="np",
                    padding="max_length",
                    truncation=True,
                    max_length=int(max_len),
                )
                samples.append({k: np.asarray(v).astype("int64") for k, v in enc.items() if k in input_names})
            return iter(samples)

        def get_next(self):
            if self._iter is None:
                self._iter = self._build()
            return next(self._iter, None)

        def rewind(self) -> None:
            self._iter = None

    return _FinbertCalibrationReader()


def _quantize_dynamic(onnx_path: str) -> Dict[str, Any]:
    from onnxruntime.quantization import QuantType, quantize_dynamic  # type: ignore

    int8_path = onnx_path.replace(".onnx", ".int8.onnx")
    quantize_dynamic(onnx_path, int8_path, weight_type=QuantType.QInt8)
    return {"int8_path": int8_path, "quantizer": "onnxruntime_dynamic"}


def _quantize_static(onnx_path: str, tokenizer: Any, texts: List[str], max_len: int) -> Dict[str, Any]:
    int8_path = onnx_path.replace(".onnx", ".int8.onnx")

    # Pre-process (shape inference + graph cleanup) improves static-quant results.
    prep_path = onnx_path
    try:
        from onnxruntime.quantization.shape_inference import quant_pre_process  # type: ignore

        prep_path = onnx_path.replace(".onnx", ".prep.onnx")
        quant_pre_process(onnx_path, prep_path)
    except Exception:
        prep_path = onnx_path  # no-op-guard: pre-process is best-effort.

    reader = _build_calibration_reader(prep_path, tokenizer, texts, max_len)

    # Prefer AMD's Quark/vai_q_onnx (NPU-optimal) when the Ryzen AI stack is present.
    try:
        import vai_q_onnx  # type: ignore

        vai_q_onnx.quantize_static(
            prep_path,
            int8_path,
            reader,
            quant_format=vai_q_onnx.QuantFormat.QDQ,
            activation_type=vai_q_onnx.QuantType.QInt8,
            weight_type=vai_q_onnx.QuantType.QInt8,
            enable_npu_cnn=True,
        )
        return {"int8_path": int8_path, "quantizer": "vai_q_onnx", "calibration_samples": len(texts)}
    except Exception:
        from onnxruntime.quantization import QuantFormat, QuantType, quantize_static  # type: ignore

        quantize_static(
            prep_path,
            int8_path,
            reader,
            quant_format=QuantFormat.QDQ,
            per_channel=True,
            weight_type=QuantType.QInt8,
        )
        return {
            "int8_path": int8_path,
            "quantizer": "onnxruntime_static",
            "calibration_samples": len(texts),
            # ORT's generic static quantizer noticeably degrades BERT-class accuracy
            # (large activation outliers). The NPU's own quantizer (vai_q_onnx /
            # Quark) is transformer-aware and is what produces an accurate INT8
            # model for the NPU. This fallback only yields a structurally-valid
            # QDQ graph; verify accuracy before trusting it.
            "accuracy_warning": "onnxruntime_static_degrades_transformer_accuracy_use_vai_q_onnx_for_npu",
        }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Export + INT8-quantize FinBERT to ONNX for the NPU.")
    parser.add_argument("--model", default=os.environ.get("FINBERT_MODEL_NAME", "ProsusAI/finbert"))
    parser.add_argument("--output-dir", default="artifacts/finbert_onnx")
    parser.add_argument(
        "--from-onnx",
        default="",
        help="Quantize this existing FP32 ONNX instead of re-exporting (export needs torch>=2.6; static quant does not).",
    )
    parser.add_argument(
        "--quant",
        choices=["static", "dynamic", "none"],
        default="dynamic",
        help=(
            "dynamic (default): accurate, weights-only, what the CPU-EP fallback serves today. "
            "static: the NPU-target format (weights+activations); accurate only with AMD's vai_q_onnx "
            "(the generic onnxruntime static fallback degrades BERT accuracy). none: FP32 only."
        ),
    )
    parser.add_argument("--calibration-file", default="", help="Text file, one calibration sample per line (static mode).")
    parser.add_argument("--max-length", type=int, default=128)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    if args.from_onnx:
        onnx_path = str(args.from_onnx)
        if not os.path.exists(onnx_path):
            raise SystemExit(f"from_onnx_missing:{onnx_path}")
    else:
        onnx_path = _export_onnx(str(args.model), str(args.output_dir))

    quant: Dict[str, Any]
    if args.quant == "none":
        quant = {"int8_path": None, "quantizer": "none"}
    elif args.quant == "dynamic":
        quant = _quantize_dynamic(onnx_path)
    else:
        tokenizer = _load_tokenizer(str(args.model), onnx_path)
        texts = _read_calibration_texts(str(args.calibration_file) or None)
        quant = _quantize_static(onnx_path, tokenizer, texts, int(args.max_length))

    final_path = quant.get("int8_path") or onnx_path
    result = {
        "ok": True,
        "model": str(args.model),
        "quant_mode": str(args.quant),
        "onnx_path": onnx_path,
        "int8_path": quant.get("int8_path"),
        "quantizer": quant.get("quantizer"),
        "calibration_samples": quant.get("calibration_samples"),
        "accuracy_warning": quant.get("accuracy_warning"),
        "use_with": {
            "FINBERT_ONNX_PATH": os.path.abspath(final_path),
            "FINBERT_BACKEND": "onnx-vitisai",
        },
    }
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print("FinBERT ONNX export: OK")
        print(f"  mode      : {args.quant}  (quantizer={quant.get('quantizer')})")
        print(f"  fp32 onnx : {onnx_path}")
        print(f"  int8 onnx : {quant.get('int8_path') or '(skipped)'}")
        if quant.get("calibration_samples"):
            print(f"  calib set : {quant.get('calibration_samples')} samples")
        if quant.get("accuracy_warning"):
            print(f"  WARNING   : {quant.get('accuracy_warning')}")
        print("  next:")
        print(f"    export FINBERT_ONNX_PATH={os.path.abspath(final_path)}")
        print("    export FINBERT_BACKEND=onnx-vitisai")
        print("    python tools/validate_npu_acceleration.py --require-ready")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
