# AtelierV: Full Creative Commissions, Escrow & Digital Goods Suite

A dual-binary Windows application written in **Rust**, sharing a single embedded **SQLite database (`vgen_local.db`)**:
- **`atelierv.exe`** &rarr; User & Artist Portal (`http://localhost:7860`)
- **`atelierv-admin.exe`** &rarr; Dedicated Admin Command Center (`http://localhost:7861`)

Both executables run out of the same directory, meaning any commission created, product purchased, review left, or persona added by the user is **instantly reflected in real-time** inside the Admin Command Center!

---

## ☁️ Zero-Local-Install (Single Download Package)

GitHub Actions builds both executables and packages them into **one unified artifact zip**:

### 1. Push Code to GitHub
```powershell
git add .
git commit -m "Bundle admin and user executables in one artifact"
git push
```

### 2. Download the Unified Suite
1. Go to your GitHub repository &rarr; **Actions** tab.
2. Select the latest run of **`Build & Release AtelierV Suite`**.
3. Under **Artifacts** at the bottom, download:
   👉 **`atelierv-suite-windows-x64.zip`**
4. Extract the zip into your folder. It contains:
   ```
   ├── atelierv.exe           # User / Artist portal
   ├── atelierv-admin.exe     # Admin Command Center
   ├── run.bat                # 1-click launcher for User portal (:7860)
   ├── run-admin.bat          # 1-click launcher for Admin portal (:7861)
   └── vgen_local.db          # Auto-generated shared SQLite database
   ```

### 3. Launching
- **Double-click [run.bat](file:///d:/Storage%20Directories/IDE%20Directories/AtelierV/run.bat)** &rarr; opens `http://localhost:7860` (Home digital marketplace, pay-what-you-want pricing, in-element milestone timelines).
- **Double-click [run-admin.bat](file:///d:/Storage%20Directories/IDE%20Directories/AtelierV/run-admin.bat)** &rarr; opens `http://localhost:7861` (Admin persona control, emergency overrides, live global event bus).

Because both `.exe` files run in the same folder, they read and write to the same `vgen_local.db` database automatically!
