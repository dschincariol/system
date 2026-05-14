"""
FILE: train_temporal_embeddings.py

Minimal entrypoint for rebuilding temporal embeddings from base event
embeddings. The heavy lifting lives in `temporal_encoder.py`; this file exists
mainly so the job system has a simple callable script.
"""

from engine.strategy.temporal_encoder import build_temporal_embeddings

if __name__ == "__main__":
    res = build_temporal_embeddings()
    print(res)
