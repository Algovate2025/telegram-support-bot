# Telegram Support Bot v2.0 - Production Ready

## Features

### 🛡️ Zuverlässigkeit
- **Outbox Pattern**: Keine Nachrichten gehen verloren, auch bei Crash/Netzwerkproblemen
- **Auto-Restart**: Bot startet automatisch nach Fehlern
- **SQLite WAL Mode**: Robuste Datenbank mit busy_timeout

### 💬 Native Experience  
- **copy_message**: Nachrichten werden 1:1 wie im Original übertragen
- **Alle Medien**: Voice, Video, Bilder, Dokumente - alles nativ

### ⚡ Schneller Workflow
- **Inline Buttons**: /inbox und /followup mit Klick-Buttons
- **Hotkeys**: /next, /last für schnelles Navigieren
- **Snooze**: Chats temporär ausblenden

### 📊 Smart Follow-ups
- VIP: 12h
- Normal: 24h
- Automatische Eskalation (fällig → dringend → überfällig)

## Setup

### Environment Variables (PFLICHT!)
```bash
export BOT_TOKEN="dein_token_hier"
export SUPPORT_GROUP_ID="-100xxxxxxxxxx"
export ADMIN_IDS="123456789,987654321"
```

### Optional
```bash
export DATA_DIR="/app/data"  # Für Railway Volume
export WELCOME_MESSAGE="Deine Begrüßung"
```

### Lokal starten
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python bot.py
```

### Railway Deployment
1. GitHub Repo erstellen
2. Dateien hochladen
3. Railway: New Project → Deploy from GitHub
4. Variables setzen (BOT_TOKEN, SUPPORT_GROUP_ID, ADMIN_IDS)
5. Volume hinzufügen: Mount path `/app/data`

## Befehle

### Inbox
- `/inbox` - Ungelesene mit Buttons
- `/all` - Alle Chats
- `/next` - Nächster ungelesener
- `/last` - Letzter Chat
- `/search` - Suchen

### Follow-Up
- `/followup` - Fällige mit Buttons
- `/done` - Erledigt
- `/skip` - Überspringen
- `/snooze [h]` - Ausblenden

### Im Topic
- `/unread` `/read` - Status
- `/vip` `/urgent` - Priorität
- `/close` - Archivieren
- `/info` `/note` - Details
- `/del` - Nachricht löschen (auf Nachricht antworten)
- `/undo [n]` - Letzte n löschen

### Templates
- `/t` - Text-Templates
- `/q` - Kurzbefehle
- `/save name` - Kurzbefehl erstellen → Nachrichten senden → `/done`

### Broadcast
- `/bc followup [text]` - An alle Follow-ups
- `/bc all [text]` - An alle
- `/bc vip [text]` - An VIPs

## Sicherheit

⚠️ **WICHTIG**: Token NIEMALS im Code speichern!

1. Token bei @BotFather regenerieren falls er je öffentlich war
2. Nur als Environment Variable setzen
3. Bot startet ohne TOKEN nicht

## Datenbank

SQLite mit WAL Mode:
- `support.db` - Hauptdatenbank
- `support.db-wal` - Write-Ahead Log
- `support.db-shm` - Shared Memory

Für Railway: Volume auf `/app/data` mounten!
