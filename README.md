# AtelierV: VGen-Style Creative Commissions & Digital Goods Hub

A dual-binary, full-stack creative suite written in **Rust**, with embedded responsive dark glassmorphic web dashboards, **SQLite DBMS (via SQLx)**, and **WebSockets**.

---

## 🏛️ Architecture: Two Separate Executables

1. **`atelierv.exe` (Port 7860 - User & Artist Portal)**
   - **Launch with:** [run.bat](file:///d:/Storage%20Directories/IDE%20Directories/AtelierV/run.bat) &rarr; opens `http://localhost:7860`
   - **Features:**
     - **Home Page Digital Marketplace:** Artists can put out digital goods with previews (static images or animated GIFs), file download packages, and helpful vendor guidelines.
     - **Curated Discovery (Not First-to-Post):** Products are ordered using a weighted algorithm factoring in average ratings, review count, and discovery scores so the first posted item doesn't just sit stuck at the top.
     - **Pay-What-You-Want Pricing:** Artists set a fixed minimum baseline price (e.g. $20). Clients can choose to pay more to support the creator, but cannot checkout below the fixed price.
     - **5-to-1 Star Review System:** Verified rating system with star breakdowns and collector comments.
     - **My Downloads Vault:** Instant digital deliverable retrieval.
     - **Custom Commission Requests:** Scoped so Clients only see what they request, Artists only see briefs put out to them.
     - **In-Card Milestone Tracking:** Clicking on any commission expands an audit timeline showing every workflow phase.
     - **Persistent Auth:** Sign in once; session automatically persists via localStorage.

2. **`atelierv-admin.exe` (Port 7861 - Dedicated Admin Command Center)**
   - **Launch with:** [run-admin.bat](file:///d:/Storage%20Directories/IDE%20Directories/AtelierV/run-admin.bat) &rarr; opens `http://localhost:7861`
   - **Features:**
     - Completely separated localhost service dedicated solely to system administration.
     - Global persona management (create, inspect, delete accounts).
     - Global commission lifecycle inspection and emergency overrides.
     - Real-time system event monitor via WebSockets.

---

## ☁️ Zero-Local-Install Setup (Build via GitHub Actions)

Since you don't have disk space locally for the Rust compiler, GitHub Actions compiles both `.exe` files in the cloud:

1. **Push your code to GitHub**:
   ```powershell
   git add .
   git commit -m "Add separate admin executable, digital marketplace, reviews, and pay-what-you-want"
   git push
   ```
2. **Download the Compiled Binaries**:
   - Go to your repo's **Actions** tab &rarr; click **`Build & Release AtelierV Executables`**.
   - Under **Artifacts**, download:
     - `atelierv-windows-x64` (extract `atelierv.exe` into this folder)
     - `atelierv-admin-windows-x64` (extract `atelierv-admin.exe` into this folder)
3. **Launch**:
   - Double-click [run.bat](file:///d:/Storage%20Directories/IDE%20Directories/AtelierV/run.bat) to launch the User/Artist Marketplace (`http://localhost:7860`).
   - Double-click [run-admin.bat](file:///d:/Storage%20Directories/IDE%20Directories/AtelierV/run-admin.bat) to launch the Admin Center (`http://localhost:7861`).
