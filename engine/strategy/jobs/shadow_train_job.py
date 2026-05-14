"""
FILE: shadow_train_job.py

Runs shadow training across a fixed set of horizons.
"""

from engine.strategy.shadow_trainer import train_shadow

HORIZONS = [60, 300, 900]

def run():
    for h in HORIZONS:
        train_shadow(
            model_name="embed_regressor",
            horizon_s=h,
            regime=None,
        )

if __name__ == "__main__":
    run()
