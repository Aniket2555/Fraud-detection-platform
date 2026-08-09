"""
PyTorch Deep Autoencoder for Unsupervised Anomaly Detection.
Includes Trimmed MSE Loss for robustness against unlabelled fraud contamination.
"""

import torch
import torch.nn as nn
import numpy as np
from sklearn.preprocessing import StandardScaler


class FraudAutoencoder(nn.Module):
    def __init__(self, input_dim: int, bottleneck_dim: int = 32):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.BatchNorm1d(128),
            nn.LeakyReLU(0.2),
            nn.Dropout(0.1),

            nn.Linear(128, 64),
            nn.BatchNorm1d(64),
            nn.LeakyReLU(0.2),
            nn.Dropout(0.1),

            nn.Linear(64, bottleneck_dim),
            nn.LeakyReLU(0.2)
        )

        self.decoder = nn.Sequential(
            nn.Linear(bottleneck_dim, 64),
            nn.BatchNorm1d(64),
            nn.LeakyReLU(0.2),

            nn.Linear(64, 128),
            nn.BatchNorm1d(128),
            nn.LeakyReLU(0.2),

            nn.Linear(128, input_dim)
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        latent = self.encoder(x)
        reconstructed = self.decoder(latent)
        return reconstructed

    def reconstruction_error(self, x: torch.Tensor) -> np.ndarray:
        """Returns per-sample MSE reconstruction error as numpy array."""
        self.eval()
        with torch.no_grad():
            reconstructed = self.forward(x)
            mse = torch.mean((x - reconstructed) ** 2, dim=1)
            return mse.cpu().numpy()


def trimmed_mse_loss(recon_x: torch.Tensor, x: torch.Tensor, trim_pct: float = 0.01) -> torch.Tensor:
    """Computes MSE loss excluding top trim_pct samples with highest loss."""
    per_sample_loss = torch.mean((x - recon_x) ** 2, dim=1)
    k = int((1.0 - trim_pct) * per_sample_loss.size(0))
    topk_loss, _ = torch.topk(per_sample_loss, k=k, largest=False)
    return torch.mean(topk_loss)


def train_autoencoder(
    X_legit_train: np.ndarray,
    input_dim: int,
    bottleneck_dim: int = 32,
    epochs: int = 50,
    batch_size: int = 512,
    lr: float = 1e-3,
    trim_pct: float = 0.01
) -> tuple:
    """Full training loop for Autoencoder. Returns (model, scaler)."""
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_legit_train)

    tensor_data = torch.tensor(X_scaled, dtype=torch.float32)
    dataset = torch.utils.data.TensorDataset(tensor_data)
    dataloader = torch.utils.data.DataLoader(dataset, batch_size=batch_size, shuffle=True)

    model = FraudAutoencoder(input_dim=input_dim, bottleneck_dim=bottleneck_dim)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=1e-5)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, patience=5, factor=0.5)

    model.train()
    for epoch in range(epochs):
        epoch_loss = 0.0
        for batch in dataloader:
            x_batch = batch[0]
            optimizer.zero_grad()
            recon = model(x_batch)
            loss = trimmed_mse_loss(recon, x_batch, trim_pct=trim_pct)
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item()

        avg_loss = epoch_loss / len(dataloader)
        scheduler.step(avg_loss)

    return model, scaler
