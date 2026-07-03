<div align="center">

<img src="assets/banner.webp" alt="ARXOS" width="880">

# ARXOS

***An Arch, reforged.***

A redistributable offensive **and** defensive security distribution.

![version](https://img.shields.io/badge/version-0.0.1-e8702a?style=for-the-badge&labelColor=0a0a0c)
![base](https://img.shields.io/badge/base-Arch-e8702a?style=for-the-badge&labelColor=0a0a0c)
![arch](https://img.shields.io/badge/x86__64-e8702a?style=for-the-badge&labelColor=0a0a0c)
![arsenal](https://img.shields.io/badge/arsenal-2840%2B%20tools-e8702a?style=for-the-badge&labelColor=0a0a0c)

**[thearxos.oxborn3.com](https://thearxos.oxborn3.com)**  ·  [Docs](https://thearxos.oxborn3.com/docs.html)  ·  [contact@oxborn3.com](mailto:contact@oxborn3.com)

</div>

---

ARXOS is a heavily tuned Arch-based system that boots fast and arrives ready for security work: a curated arsenal under one Weapons menu, a self-healing package manager, a refined desktop, and a graphical installer. Every shipped feature is an ARXOS-native tool with its own repo, pinned to `0.0.1`, that updates itself.

## What's inside

- **The Weapons arsenal.** 2840+ tools for recon, exploitation, defense and research, sorted into categories. Install a whole category in one line: `arx install weaponsCat <category>` (or `weaponsCat all`).
- **`arx`.** A self-healing package manager over pacman. One `arx update` upgrades the system and every ARXOS tool in a single pass, and it repairs keyring, mirror, lock and database errors on its own.
- **`arxguard`.** A zero-trust command guard in your shell: it blocks homograph URLs, decode-and-exec, pipe-to-root-shell, credential theft and destructive commands before they run.
- **Control Center (`arxctl`).** A real-time GUI for updates, kernels, CPU governor, Tor routing, services, snapshots and wallpaper.
- **`droidB`.** A graphical Android toolkit (ADB, fastboot, Frida, flashing).
- **`t0rpoiz0n`.** One-key whole-system Tor routing with DNS-leak, MAC and IPv6 hardening.
- A tuned performance kernel, hardened browsers, hypervisor auto-resize, and a source-built installer.

## Install

1. Download the latest `arxos-0.0.1-x86_64.iso`.
2. Flash it: `dd if=arxos.iso of=/dev/sdX bs=4M status=progress`.
3. Boot the live ISO (it auto-resizes to any hypervisor and runs on bare metal), then launch the installer.

## The toolset

Each tool is its own repo and self-updates:

| Tool | Role |
|------|------|
| `arx` | self-healing package manager + arsenal installer |
| `arxctl` | Control Center GUI |
| `arxguard` | zero-trust command guard |
| `arxupd` | feature-repo self-updater |
| `droidB` | Android toolkit |
| `t0rpoiz0n` | whole-system Tor routing |
| `cpu-governor` | CPU governor control |
| `arxos-vm-resize` | hypervisor auto-resize |

---

<div align="center">
<i>ARXOS 0.0.1. Built on Arch, of course.</i><br>
<a href="https://oxborn3.com">0xb0rn3 · oxborn3.com</a>
</div>
