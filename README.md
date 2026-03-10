# Telegram Scanner Bot (Mail & Fortnite)

This bot is a comprehensive tool for scanning mail access and Fortnite accounts.

## Features
- **Mail Access Checker**: Supports Microsoft (Outlook, Hotmail) and IMAP (Gmail, Yahoo, etc.).
- **Fortnite Checker**: Full capture including Skins, V-Bucks, and Account ID.
- **Admin Panel**: Create VIP codes for users.
- **Automatic Cleaning**: Removes duplicates and formats combo lists.

## Deployment on Railway

1. **Upload to GitHub**:
   - Create a new private repository on GitHub.
   - Push all files from the `deploy_bot` folder to your repository.

2. **Connect to Railway**:
   - Create a new project on [Railway.app](https://railway.app/).
   - Select "Deploy from GitHub repo".
   - Choose your repository.

3. **Set Environment Variables**:
   Go to **Variables** in your Railway project and add:
   - `TELEGRAM_BOT_TOKEN`: Your bot token from @BotFather.
   - `TELEGRAM_GROUP_ID`: The ID of the group where hits will be sent.
   - `ADMIN_ID`: Your Telegram User ID.
   - `CONTROL_GROUP_ID`: (Optional) Group for admin controls.

## Files Structure
- `bot.py`: Main bot script.
- `fortnite/`: Contains Fortnite logic and skins database.
- `requirements.txt`: Python dependencies.
- `Procfile`: Start command for Railway.
