# AtelierV: VGen-Style Single-Binary Local Hub

A single-binary creative commission, escrow, and character IP rights management application built with **Rust**, **Axum**, **SQLite (via SQLx)**, and **WebSockets**.

---

## ☁️ Zero-Local-Install Setup (Build via GitHub Actions)

If you have limited disk space and **do not** want to install Rust (~5-10 GB for compiler + C++ build tools), GitHub Actions can compile it for you in the cloud for free:

### 1. Push to GitHub
Commit and push this repository to your GitHub account:
```powershell
git init
git add .
git commit -m "Initial commit for AtelierV"
git branch -M main
git remote add origin https://github.com/<YOUR_USERNAME>/<YOUR_REPO>.git
git push -u origin main
```

### 2. Cloud Compilation Runs Automatically
- Open your GitHub repository in your web browser.
- Go to the **Actions** tab.
- Click the workflow: **`Build & Release AtelierV Executable`**.
- It runs on GitHub's Windows cloud runner, compiles the release single-binary with embedded HTML dashboard, and attaches it as an artifact.

### 3. Download & Run
1. In the completed workflow run, scroll down to **Artifacts** and download **`atelierv-windows-x64`**.
2. Extract `atelierv.exe` into this project folder (right beside [run.bat](file:///d:/Storage%20Directories/IDE%20Directories/AtelierV/run.bat)).
3. Double-click [run.bat](file:///d:/Storage%20Directories/IDE%20Directories/AtelierV/run.bat) (or run `./atelierv.exe`).
4. It will immediately start the server and open your default browser to `http://localhost:7860`.

---

## 🛠 Local Build (Alternative, If You Have Rust)

If you ever have Rust and disk space available:
```powershell
cargo run --release
```

---

## 📁 Project Structure

```
AtelierV/
├── .github/
│   └── workflows/
│       └── build.yml      # Automated GitHub Actions cloud compiler for Windows
├── .gitignore             # Excludes databases, logs, and build artifacts from git
├── Cargo.toml             # Rust dependencies (Axum, Tokio, SQLx, Open, Serde)
├── run.bat                # 1-click launcher for the downloaded atelierv.exe
├── README.md              # Instructions & Architecture guide
└── src/
    ├── main.rs            # Embedded SQLite, auto-migrations, REST API, WebSockets
    └── index.html         # Embedded dark glassmorphic dashboard (single-binary asset)
```
