# AtelierV: VGen-Style Single-Binary Creative Hub

A full-stack commission, escrow, and digital marketplace platform built with **Rust**, **Axum**, **SQLite (SQLx)**, and **WebSockets**.

---

## Three Binaries, One Shared Database

| Executable | Port | Purpose |
|---|---|---|
| `atelierv.exe` | 7860 | User Portal — Client/Artist login, digital market, commissions |
| `atelierv-admin.exe` | 7861 | Web Admin — Full user management, commission oversight, live events |
| `atelierv-admin-cli.exe` | N/A | Terminal Admin CLI — Non-web, direct DB access for power users |

All three connect to the same `vgen_local.db` SQLite file in the same folder.

---

## Cloud Build via GitHub Actions (No Local Rust Required)

1. **Push this folder** to a GitHub repository
2. Go to **Actions** tab ? click the latest **"Build & Release AtelierV Suite"** run
3. Scroll to **Artifacts** section ? download **`atelierv-suite-windows-x64`**
4. Unzip — you get all three `.exe` files plus the launch scripts

## Running

- **User portal:** double-click `run.bat` ? opens `http://localhost:7860`
- **Web admin:** double-click `run-admin.bat` ? opens `http://localhost:7861`
- **Terminal admin:** double-click `run-admin-cli.bat` ? interactive console menu

> Run the user portal **first** so the database tables are created, then launch admin in a second terminal.

---

## Features

- **Login / Signup** with persistent session (localStorage) — no re-login needed
- **Role-based views:** Client, Artist, Admin scopes
- **Digital Marketplace:** Artists publish products with image/GIF previews, fixed base price, pay-what-you-want for clients, 5-star reviews
- **Product Detail Modal:** product media ? vendor info box ? star reviews/comments on left; purchase panel on right
- **Custom Commissions:** briefs, milestone audit trail per card, escrow locking
- **In-card Progress Drawer:** click any commission card to expand its timeline
- **Tester Accounts:** admin can grant All-Download Pass — no purchase popup, instant vault access
- **Admin Toggle Access:** grant/revoke All-Download Pass per user without deleting account
- **WebSocket Real-time Sync:** live events pushed to all connected browsers
- **Admin 4-second auto-poll:** stays in sync with user portal DB changes even across processes
