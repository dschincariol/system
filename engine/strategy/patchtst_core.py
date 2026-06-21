"""Dependency-light PatchTST network module."""

from __future__ import annotations

import torch
import torch.nn as nn


class PatchTST(nn.Module):
    """Compact PatchTST encoder using patch tokens and a Transformer encoder."""

    def __init__(
        self,
        *,
        seq_len: int,
        n_features: int,
        n_horizons: int,
        patch_len: int = 16,
        stride: int = 8,
        d_model: int = 64,
        n_layers: int = 3,
        n_heads: int = 4,
        dropout: float = 0.1,
    ) -> None:
        super().__init__()
        self.seq_len = int(seq_len)
        self.n_features = int(n_features)
        self.n_horizons = int(n_horizons)
        self.patch_len = int(patch_len)
        self.stride = int(stride)
        self.d_model = int(d_model)
        self.n_layers = int(n_layers)
        self.n_heads = int(n_heads)
        self.dropout = float(dropout)
        if self.patch_len <= 0 or self.stride <= 0:
            raise ValueError("patch_len_and_stride_must_be_positive")
        if self.seq_len < self.patch_len:
            raise ValueError("seq_len_must_cover_one_patch")
        self.n_patches = 1 + ((self.seq_len - self.patch_len) // self.stride)
        self.patch_projection = nn.Linear(self.patch_len * self.n_features, self.d_model)
        self.position = nn.Parameter(torch.zeros(1, self.n_patches, self.d_model))
        layer = nn.TransformerEncoderLayer(
            d_model=self.d_model,
            nhead=self.n_heads,
            dim_feedforward=self.d_model * 4,
            dropout=self.dropout,
            batch_first=True,
            activation="gelu",
            norm_first=True,
        )
        self.encoder = nn.TransformerEncoder(layer, num_layers=self.n_layers)
        self.norm = nn.LayerNorm(self.d_model)
        self.head = nn.Linear(self.d_model, self.n_horizons)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        if x.ndim != 3:
            raise ValueError("patchtst_forward_requires_3d")
        patches = x.unfold(dimension=1, size=self.patch_len, step=self.stride)
        patches = patches.permute(0, 1, 3, 2).contiguous()
        patches = patches.reshape(x.shape[0], self.n_patches, self.patch_len * self.n_features)
        tokens = self.patch_projection(patches) + self.position
        encoded = self.encoder(tokens)
        pooled = self.norm(encoded.mean(dim=1))
        return self.head(pooled)
